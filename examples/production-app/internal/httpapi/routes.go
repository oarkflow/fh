package httpapi

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	appauth "github.com/oarkflow/fh/examples/production-app/internal/auth"
	appconfig "github.com/oarkflow/fh/examples/production-app/internal/config"
	appdb "github.com/oarkflow/fh/examples/production-app/internal/database"
	appsecurity "github.com/oarkflow/fh/examples/production-app/internal/security"
	"github.com/oarkflow/authz"
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/contract"
	"github.com/oarkflow/fh/mw/securetransport"
	"github.com/oarkflow/fh/mw/session"
	"github.com/oarkflow/squealx"
)

type assetManifest struct {
	Assets map[string]struct {
		Integrity string `json:"integrity"`
	} `json:"assets"`
}

func Register(app *fh.App, cfg appconfig.Config, sessions *session.SessionManager, transport *securetransport.Transport, engine *authz.Engine, grants *appauth.GrantStore, guard *appsecurity.Guard, store *appdb.Store, responsePublicKey string) error {
	manifest, err := loadManifest("web/wasm/asset-manifest.json")
	if err != nil {
		return err
	}
	// /assets serves hand-edited, unhashed filenames (app.js, app.css), so it must
	// always revalidate — a time-based cache here leaves the browser holding a stale
	// script indefinitely after any edit or deploy, with no way to bust it.
	app.Static("/assets", "web/public", fh.StaticConfig{CacheControl: "no-cache", CacheDuration: time.Minute})
	app.Static("/wasm", "web/wasm", fh.StaticConfig{MaxAge: 31536000, CacheDuration: time.Minute})
	app.Get("/", func(c fh.Ctx) error { return renderIndex(c, cfg.Production) }).Name("home").Tag("web")
	app.Get("/healthz", func(c fh.Ctx) error { return c.JSON(fh.Map{"status": "ok"}) }).Name("health").Tag("operations")
	app.Get("/auth/session", func(c fh.Ctx) error {
		c.Set("Cache-Control", "no-store")
		principal, ok := appauth.Principal(c)
		if !ok {
			return c.JSON(fh.Map{"authenticated": false})
		}
		return c.JSON(fh.Map{"authenticated": true, "userID": principal.ID})
	}).Name("session-status").Tag("authentication")

	app.Post("/auth/login", contract.New(contract.Config{Methods: []string{"POST"}, ContentTypes: []string{"application/json"}, MaxBodyBytes: 16 << 10}), func(c fh.Ctx) error {
		if !sameOrigin(c, cfg.AllowedOrigins) {
			return fh.NewHTTPError(fh.StatusForbidden, "ORIGIN_REJECTED", "same-origin login required")
		}
		user, password, err := appauth.DecodeLogin(c)
		if err != nil {
			return err
		}
		if err := appauth.Login(c, sessions, store, user, password, cfg.LoginUser, cfg.LoginPassword, "admin"); err != nil {
			// A failed login is exactly the signal the brute-force/credential-
			// stuffing detector needs; see security/policy/tcpguard.bcl's
			// auth-brute-force rule. Ignore emission errors - anomaly
			// detection must never block the auth error response itself -
			// but a confirmed block DOES override the normal 401 so repeated
			// attempts get a distinct, rate-limit-shaped response instead of
			// quietly continuing to accept guesses.
			if blocked, evalErr := guard.EmitAuthEvent(c, "auth.login_failed", user); evalErr == nil && blocked {
				return fh.NewHTTPError(fh.StatusTooManyRequests, "TOO_MANY_ATTEMPTS", "too many failed login attempts")
			}
			return err
		}
		_, _ = guard.EmitAuthEvent(c, "auth.login_success", user)
		c.Flash("message", "Signed in successfully.")
		return c.JSON(fh.Map{"authenticated": true, "message": "Signed in successfully."})
	}).Name("login").Tag("authentication")
	app.Post("/auth/logout", func(c fh.Ctx) error {
		if !sameOrigin(c, cfg.AllowedOrigins) {
			return fh.NewHTTPError(fh.StatusForbidden, "ORIGIN_REJECTED", "same-origin logout required")
		}
		return sessions.Destroy(c, session.Get(c))
	}).Name("logout").Tag("authentication")
	app.Post("/secure-config.json", func(c fh.Ctx) error {
		if !sameOrigin(c, cfg.AllowedOrigins) {
			return fh.NewHTTPError(fh.StatusForbidden, "ORIGIN_REJECTED", "same-origin bootstrap required")
		}
		principal, ok := appauth.Principal(c)
		if !ok {
			return fh.NewHTTPError(fh.StatusUnauthorized, "LOGIN_REQUIRED", "login required")
		}
		grant, err := grants.Issue(principal.ID, session.Get(c).ID)
		if err != nil {
			return fh.NewHTTPError(fh.StatusServiceUnavailable, "GRANT_UNAVAILABLE", "registration grant unavailable")
		}
		c.Set("Cache-Control", "no-store")
		return c.JSON(fh.Map{
			"baseURL": requestOrigin(c, cfg.Origin), "pinnedServerKey": transport.PublicKeyBase64(), "pinnedServerKeyID": transport.KeyID(),
			"responseSigningPublicKey": responsePublicKey, "responseSigningKeyID": cfg.ResponseSigningKeyID,
			"requireResponseSignature": true, "requireEmbeddedTrust": cfg.Production,
			"registrationToken": grant, "wasmURL": "/wasm/securefetch.wasm", "wasmExecURL": "/wasm/wasm_exec.js",
			"wasmIntegrity": manifest.Assets["securefetch.wasm"].Integrity, "wasmExecIntegrity": manifest.Assets["wasm_exec.js"].Integrity,
			"requireAssetIntegrity": true,
		})
	}).Name("secure-config").Tag("authentication")

	api := app.Group("/api", appauth.Require, appauth.PolicyMiddleware(engine))
	api.Get("/me", func(c fh.Ctx) error {
		p, _ := appauth.Principal(c)
		secure, _ := securetransport.SessionFromContext(c)
		return c.JSON(fh.Map{"userID": p.ID, "roles": p.Roles, "securePrincipal": secure.Principal})
	}).Name("profile").Tag("api")
	api.Post("/echo", contract.New(contract.Config{Methods: []string{"POST"}, ContentTypes: []string{"application/json"}, MaxBodyBytes: 64 << 10}), func(c fh.Ctx) error {
		var body map[string]any
		if err := json.Unmarshal(c.BodyCopy(), &body); err != nil {
			return fh.NewHTTPError(fh.StatusBadRequest, "INVALID_JSON", "invalid JSON")
		}
		return c.JSON(fh.Map{"accepted": true, "body": body})
	}).Name("echo").Tag("api")
	registerNoteRoutes(api, store, engine, guard)
	return nil
}

// registerNoteRoutes demonstrates the full stack together: squealx for a
// paginated, indexed read plus retried writes; authz for a per-row ABAC
// ownership check (RBAC alone can't express "only your own notes" - it has
// no idea which row is being touched until it's fetched); and tcpguard for
// business-logic anomaly detection on the write path (see
// security.Guard.EmitBusinessEvent and the note-creation-velocity rule).
func registerNoteRoutes(api *fh.Group, store *appdb.Store, engine *authz.Engine, guard *appsecurity.Guard) {
	api.Get("/notes", func(c fh.Ctx) error {
		principal, _ := appauth.Principal(c)
		paging := squealx.Paging{OrderBy: []string{"created_at DESC"}, Limit: notesPageLimit(c), Page: notesPage(c)}
		page := store.Notes.Paginate(c.Context(), paging, map[string]any{"user_id": principal.ID})
		if page.Error != nil {
			return fh.NewHTTPError(fh.StatusInternalServerError, "NOTES_LIST_FAILED", "could not list notes")
		}
		return c.JSON(page)
	}).Name("notes-list").Tag("api")

	api.Post("/notes", contract.New(contract.Config{Methods: []string{"POST"}, ContentTypes: []string{"application/json"}, MaxBodyBytes: 64 << 10}), func(c fh.Ctx) error {
		principal, _ := appauth.Principal(c)
		var input struct {
			Title string `json:"title"`
			Body  string `json:"body"`
		}
		if err := json.Unmarshal(c.BodyCopy(), &input); err != nil || strings.TrimSpace(input.Title) == "" {
			return fh.NewHTTPError(fh.StatusBadRequest, "INVALID_NOTE", "title is required")
		}
		if blocked, err := guard.EmitBusinessEvent(c, "note.create", 0); err == nil && blocked {
			return fh.NewHTTPError(fh.StatusTooManyRequests, "ANOMALY_DETECTED", "too many notes created too quickly")
		}
		note := appdb.Note{ID: newID(), UserID: principal.ID, Title: input.Title, Body: input.Body}
		if err := store.Notes.Create(c.Context(), &note); err != nil {
			return fh.NewHTTPError(fh.StatusInternalServerError, "NOTE_CREATE_FAILED", "could not create note")
		}
		return c.Status(fh.StatusCreated).JSON(note)
	}).Name("notes-create").Tag("api")

	api.Put("/notes/:id", contract.New(contract.Config{Methods: []string{"PUT"}, ContentTypes: []string{"application/json"}, MaxBodyBytes: 64 << 10}), func(c fh.Ctx) error {
		principal, _ := appauth.Principal(c)
		note, err := store.Notes.First(c.Context(), map[string]any{"id": c.Param("id")})
		if err != nil || note.ID == "" {
			return fh.NewHTTPError(fh.StatusNotFound, "NOTE_NOT_FOUND", "note not found")
		}
		if err := appauth.AuthorizeOwned(c, engine, principal, "PUT", "note", note.ID, note.UserID); err != nil {
			return err
		}
		var input struct {
			Title string `json:"title"`
			Body  string `json:"body"`
		}
		if err := json.Unmarshal(c.BodyCopy(), &input); err != nil || strings.TrimSpace(input.Title) == "" {
			return fh.NewHTTPError(fh.StatusBadRequest, "INVALID_NOTE", "title is required")
		}
		note.Title, note.Body, note.UpdatedAt = input.Title, input.Body, time.Now().UTC().Format(time.RFC3339)
		if err := store.Notes.Update(c.Context(), &note, map[string]any{"id": note.ID}); err != nil {
			return fh.NewHTTPError(fh.StatusInternalServerError, "NOTE_UPDATE_FAILED", "could not update note")
		}
		return c.JSON(note)
	}).Name("notes-update").Tag("api")

	api.Delete("/notes/:id", func(c fh.Ctx) error {
		principal, _ := appauth.Principal(c)
		note, err := store.Notes.First(c.Context(), map[string]any{"id": c.Param("id")})
		if err != nil || note.ID == "" {
			return fh.NewHTTPError(fh.StatusNotFound, "NOTE_NOT_FOUND", "note not found")
		}
		if err := appauth.AuthorizeOwned(c, engine, principal, "DELETE", "note", note.ID, note.UserID); err != nil {
			return err
		}
		if err := store.Notes.Delete(c.Context(), &note); err != nil {
			return fh.NewHTTPError(fh.StatusInternalServerError, "NOTE_DELETE_FAILED", "could not delete note")
		}
		return c.Status(fh.StatusNoContent).Send(nil)
	}).Name("notes-delete").Tag("api")
}

// notesPageLimit/notesPage keep list pages small and indexed (see the
// idx_notes_user_id_created_at index in internal/database) regardless of how
// large a user's note collection grows - a client can never force an
// unbounded scan by omitting or inflating these query parameters.
const (
	notesDefaultPageLimit = 25
	notesMaxPageLimit     = 100
)

func notesPageLimit(c fh.Ctx) int {
	limit, err := strconv.Atoi(c.Query("limit"))
	if err != nil || limit <= 0 {
		return notesDefaultPageLimit
	}
	if limit > notesMaxPageLimit {
		return notesMaxPageLimit
	}
	return limit
}

func notesPage(c fh.Ctx) int {
	page, err := strconv.Atoi(c.Query("page"))
	if err != nil || page <= 0 {
		return 1
	}
	return page
}

func newID() string {
	var raw [16]byte
	_, _ = rand.Read(raw[:])
	return hex.EncodeToString(raw[:])
}

// SPL secure mode rejects every script element, including a fixed same-origin
// module reference. Validate the template first, then append this constant,
// application-owned boot tag on the response path. No template or request data
// can affect the tag, and CSP restricts script execution to this origin.
func renderIndex(c fh.Ctx, production bool) error {
	c.AddBodyTransform(func(body []byte) ([]byte, error) {
		closing := []byte("</body>")
		if !bytes.Contains(body, closing) {
			return nil, fmt.Errorf("index template is missing </body>")
		}
		boot := []byte(`<script src="/assets/app.js" type="module"></script></body>`)
		return bytes.Replace(body, closing, boot, 1), nil
	})
	return c.Render("index.html", map[string]any{"Production": production, "message": ""})
}

func loadManifest(path string) (assetManifest, error) {
	var out assetManifest
	raw, err := os.ReadFile(path)
	if err != nil {
		return out, fmt.Errorf("load WASM asset manifest: %w", err)
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, fmt.Errorf("decode WASM asset manifest: %w", err)
	}
	if out.Assets["securefetch.wasm"].Integrity == "" || out.Assets["wasm_exec.js"].Integrity == "" {
		return out, fmt.Errorf("WASM asset manifest is incomplete")
	}
	return out, nil
}

func sameOrigin(c fh.Ctx, allowedOrigins []string) bool {
	origin := strings.TrimRight(strings.TrimSpace(c.Get("Origin")), "/")
	if origin == "" || strings.EqualFold(c.Get("Sec-Fetch-Site"), "cross-site") {
		return false
	}
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Host == "" || !strings.EqualFold(parsed.Host, c.Get("Host")) {
		return false
	}
	for _, allowed := range allowedOrigins {
		if strings.EqualFold(origin, allowed) {
			return true
		}
	}
	return false
}

func requestOrigin(c fh.Ctx, fallback string) string {
	if origin := strings.TrimRight(strings.TrimSpace(c.Get("Origin")), "/"); origin != "" {
		return origin
	}
	return fallback
}
