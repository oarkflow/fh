package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/mail"
	"os"
	"strings"
	"time"

	"github.com/oarkflow/fh/mw/deviceauth"
)

//go:embed web/*
var browserAssets embed.FS

func serve(args []string) {
	flags := flag.NewFlagSet("server", flag.ExitOnError)
	addr := flags.String("addr", "127.0.0.1:8093", "loopback listen address")
	_ = flags.Parse(args)
	if !strings.HasPrefix(*addr, "127.0.0.1:") && !strings.HasPrefix(*addr, "localhost:") && !strings.HasPrefix(*addr, "[::1]:") {
		log.Fatal("this demo must listen on loopback only")
	}
	adminToken := strings.TrimSpace(os.Getenv("FH_DEVICEAUTH_ADMIN_TOKEN"))
	if adminToken == "" {
		log.Fatal("FH_DEVICEAUTH_ADMIN_TOKEN must be set to authorize invite creation")
	}
	mailer, err := newVerificationMailer()
	if err != nil {
		log.Fatal(err)
	}
	signingKey := make([]byte, 32)
	if _, err := rand.Read(signingKey); err != nil {
		log.Fatal(err)
	}
	devices := deviceauth.NewMemoryDevices(10_000)
	manager, err := deviceauth.New(deviceauth.Config{SigningKey: signingKey, Devices: devices, Replay: deviceauth.NewMemoryReplayGuard(100_000), MaxChallenges: 10_000, CheckDeviceEachRequest: true})
	if err != nil {
		log.Fatal(err)
	}
	accounts, pending, invites, requests := newAccountStore(), newPendingStore(), newInviteStore(), newRegistrationRequestStore()
	mux := http.NewServeMux()

	issueInvite := func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			UserID string `json:"user_id"`
		}
		if !readJSON(w, r, &in) {
			return
		}
		if !validUserID(in.UserID) || accounts.exists(in.UserID) {
			writeError(w, http.StatusConflict, "user ID unavailable")
			return
		}
		code, expires, ok := invites.create(in.UserID, 15*time.Minute)
		if !ok {
			writeError(w, http.StatusServiceUnavailable, "invite capacity unavailable")
			return
		}
		writeJSON(w, http.StatusCreated, map[string]string{"user_id": in.UserID, "invite_code": code, "expires_at": expires.UTC().Format(time.RFC3339)})
	}
	mux.HandleFunc("POST /admin/invites", func(w http.ResponseWriter, r *http.Request) {
		provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !constantTimeEqual(provided, adminToken) {
			writeError(w, http.StatusUnauthorized, "invite creation denied")
			return
		}
		issueInvite(w, r)
	})
	mux.HandleFunc("POST /auth/registration-requests", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			UserID string `json:"user_id"`
			Email  string `json:"email"`
		}
		if !readJSON(w, r, &in) {
			return
		}
		if reason := analyzeRegistrationRequest(in.UserID, strings.TrimSpace(in.Email), accounts); reason != "" {
			writeError(w, http.StatusUnprocessableEntity, reason)
			return
		}
		id := randomIDOrFatal(32)
		code, err := newVerificationCode()
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "verification is temporarily unavailable")
			return
		}
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		if !requests.create(id, in.UserID, strings.TrimSpace(in.Email), code, host) {
			writeError(w, http.StatusTooManyRequests, "a request is already active for this account or this device reached its request limit")
			return
		}
		if err := mailer.send(strings.TrimSpace(in.Email), code); err != nil {
			requests.delete(id)
			log.Printf("send registration verification email failed: %v", err)
			writeError(w, http.StatusServiceUnavailable, "could not send verification email; try again later")
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"request_id": id, "status": "verification_required", "message": "Enter the code sent to your email. Successful verification automatically approves eligible requests."})
	})
	mux.HandleFunc("GET /auth/registration-requests/{id}", func(w http.ResponseWriter, r *http.Request) {
		request, ok := requests.status(r.PathValue("id"))
		if !ok {
			writeError(w, http.StatusNotFound, "registration request expired or unavailable")
			return
		}
		if !request.approved {
			writeJSON(w, http.StatusOK, map[string]string{"status": "verification_required"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "approved", "user_id": request.userID, "invite_code": request.inviteCode})
	})
	mux.HandleFunc("POST /auth/registration-requests/{id}/verify", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Code string `json:"code"`
		}
		if !readJSON(w, r, &in) {
			return
		}
		id := r.PathValue("id")
		if existing, ok := requests.status(id); ok && existing.approved {
			writeJSON(w, http.StatusOK, map[string]string{"status": "approved", "user_id": existing.userID, "invite_code": existing.inviteCode})
			return
		}
		request, ok := requests.verify(id, strings.TrimSpace(in.Code))
		if !ok {
			writeError(w, http.StatusUnauthorized, "verification code is invalid, expired, or locked")
			return
		}
		if reason := analyzeRegistrationRequest(request.userID, request.email, accounts); reason != "" {
			writeJSON(w, http.StatusForbidden, map[string]string{"status": "rejected", "reason": reason})
			return
		}
		code, _, ok := invites.create(request.userID, 15*time.Minute)
		if !ok {
			writeError(w, http.StatusServiceUnavailable, "automatic approval is temporarily unavailable")
			return
		}
		if !requests.approve(id, code) {
			writeError(w, http.StatusConflict, "registration request expired; request access again")
			return
		}
		log.Printf("registration auto-approved after email verification and account/rate checks (request %s)", id)
		writeJSON(w, http.StatusOK, map[string]string{"status": "approved", "user_id": request.userID, "invite_code": code, "reason": "Email ownership verified and registration checks passed."})
	})

	startEnrollment := func(kind, userID, oldDevice string) (string, string, string, string, error) {
		deviceID := randomIDOrFatal(16)
		recoveryCode := ""
		if kind == "register" || kind == "recover" {
			recoveryCode = randomIDOrFatal(32)
		}
		challengeID, challenge, err := manager.BeginEnrollment(userID, deviceID)
		if err != nil {
			return "", "", "", "", err
		}
		flow := pendingFlow{kind: kind, userID: userID, deviceID: deviceID, oldDeviceID: oldDevice, recoveryCode: recoveryCode, expires: time.Now().Add(2 * time.Minute)}
		if !pending.put(challengeID, flow) {
			return "", "", "", "", errors.New("pending flow capacity exceeded")
		}
		return challengeID, challenge, deviceID, recoveryCode, nil
	}
	finishEnrollment := func(challengeID, signature string, publicKey ed25519.PublicKey, wantKind string) (pendingFlow, error) {
		flow, ok := pending.get(challengeID)
		if !ok || flow.kind != wantKind {
			return pendingFlow{}, errors.New("invalid or expired enrollment flow")
		}
		if err := manager.FinishEnrollment(challengeID, signature, publicKey); err != nil {
			pending.delete(challengeID)
			return pendingFlow{}, err
		}
		committed := false
		switch flow.kind {
		case "register":
			committed = accounts.create(flow.userID, flow.deviceID, flow.recoveryCode) == nil
		case "recover":
			committed = accounts.replaceDevice(flow.userID, flow.oldDeviceID, flow.deviceID, flow.recoveryCode, true)
		case "change":
			committed = accounts.replaceDevice(flow.userID, flow.oldDeviceID, flow.deviceID, "", false)
		}
		if !committed {
			_ = devices.Delete(flow.deviceID)
			pending.delete(challengeID)
			return pendingFlow{}, errors.New("account state changed; restart the flow")
		}
		if flow.oldDeviceID != "" {
			_ = devices.Delete(flow.oldDeviceID)
		}
		pending.delete(challengeID)
		return flow, nil
	}
	readProof := func(w http.ResponseWriter, r *http.Request) (string, string, ed25519.PublicKey, bool) {
		var in struct {
			ChallengeID string `json:"challenge_id"`
			Signature   string `json:"signature"`
			PublicKey   string `json:"public_key"`
		}
		if !readJSON(w, r, &in) {
			return "", "", nil, false
		}
		key, err := base64.RawURLEncoding.DecodeString(in.PublicKey)
		if err != nil || len(key) != ed25519.PublicKeySize {
			writeError(w, http.StatusBadRequest, "invalid public key")
			return "", "", nil, false
		}
		return in.ChallengeID, in.Signature, ed25519.PublicKey(key), true
	}

	mux.HandleFunc("POST /auth/register/begin", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			UserID     string `json:"user_id"`
			InviteCode string `json:"invite_code"`
		}
		if !readJSON(w, r, &in) {
			return
		}
		if !validUserID(in.UserID) || accounts.exists(in.UserID) {
			writeError(w, http.StatusConflict, "user ID unavailable")
			return
		}
		if !invites.consume(in.UserID, in.InviteCode) {
			writeError(w, http.StatusUnauthorized, "registration invite invalid or expired")
			return
		}
		id, challenge, deviceID, _, err := startEnrollment("register", in.UserID, "")
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "registration unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"user_id": in.UserID, "device_id": deviceID, "challenge_id": id, "challenge": challenge})
	})
	mux.HandleFunc("POST /auth/register/finish", func(w http.ResponseWriter, r *http.Request) {
		id, sig, key, ok := readProof(w, r)
		if !ok {
			return
		}
		flow, err := finishEnrollment(id, sig, key, "register")
		if err != nil {
			writeError(w, http.StatusUnauthorized, "registration proof rejected")
			return
		}
		writeJSON(w, http.StatusCreated, map[string]string{"user_id": flow.userID, "device_id": flow.deviceID, "recovery_code": flow.recoveryCode})
	})
	mux.HandleFunc("POST /auth/login/begin", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			UserID   string `json:"user_id"`
			DeviceID string `json:"device_id"`
		}
		if !readJSON(w, r, &in) {
			return
		}
		deviceID, exists := accounts.device(in.UserID)
		if !exists || deviceID != in.DeviceID {
			writeError(w, http.StatusUnauthorized, "login could not be started")
			return
		}
		id, challenge, err := manager.BeginLoginForUser(in.UserID, in.DeviceID)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "login could not be started")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"challenge_id": id, "challenge": challenge})
	})
	mux.HandleFunc("POST /auth/login/finish", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			ChallengeID string `json:"challenge_id"`
			Signature   string `json:"signature"`
		}
		if !readJSON(w, r, &in) {
			return
		}
		session, err := manager.FinishLogin(in.ChallengeID, in.Signature)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "login proof rejected")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"session": session})
	})

	mux.HandleFunc("POST /auth/recovery/begin", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			UserID       string `json:"user_id"`
			RecoveryCode string `json:"recovery_code"`
		}
		if !readJSON(w, r, &in) {
			return
		}
		oldDevice, ok := accounts.verifyRecovery(in.UserID, in.RecoveryCode)
		if !ok {
			writeError(w, http.StatusUnauthorized, "recovery could not be started")
			return
		}
		id, challenge, deviceID, _, err := startEnrollment("recover", in.UserID, oldDevice)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "recovery unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"device_id": deviceID, "challenge_id": id, "challenge": challenge})
	})
	mux.HandleFunc("POST /auth/recovery/finish", func(w http.ResponseWriter, r *http.Request) {
		id, sig, key, ok := readProof(w, r)
		if !ok {
			return
		}
		flow, err := finishEnrollment(id, sig, key, "recover")
		if err != nil {
			writeError(w, http.StatusUnauthorized, "recovery proof rejected")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"user_id": flow.userID, "device_id": flow.deviceID, "recovery_code": flow.recoveryCode})
	})

	protected := func(h http.HandlerFunc) http.Handler { return manager.Middleware(h) }
	mux.Handle("POST /auth/change/begin", protected(func(w http.ResponseWriter, r *http.Request) {
		user, okUser := deviceauth.Subject(r.Context())
		oldDevice, okDevice := deviceauth.DeviceID(r.Context())
		stored, exists := accounts.device(user)
		if !okUser || !okDevice || !exists || stored != oldDevice {
			writeError(w, http.StatusUnauthorized, "device change denied")
			return
		}
		id, challenge, deviceID, _, err := startEnrollment("change", user, oldDevice)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "device change unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"device_id": deviceID, "challenge_id": id, "challenge": challenge})
	}))
	mux.Handle("POST /auth/change/finish", protected(func(w http.ResponseWriter, r *http.Request) {
		id, sig, key, ok := readProof(w, r)
		if !ok {
			return
		}
		user, _ := deviceauth.Subject(r.Context())
		oldDevice, _ := deviceauth.DeviceID(r.Context())
		flow, exists := pending.get(id)
		if !exists || flow.userID != user || flow.oldDeviceID != oldDevice {
			writeError(w, http.StatusUnauthorized, "device change denied")
			return
		}
		flow, err := finishEnrollment(id, sig, key, "change")
		if err != nil {
			writeError(w, http.StatusUnauthorized, "device change proof rejected")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"user_id": flow.userID, "device_id": flow.deviceID})
	}))
	mux.Handle("GET /api/me", manager.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, _ := deviceauth.Subject(r.Context())
		writeJSON(w, http.StatusOK, map[string]string{"user_id": user, "message": "authenticated device request accepted"})
	})))
	webRoot, err := fs.Sub(browserAssets, "web")
	if err != nil {
		log.Fatal(err)
	}
	mux.Handle("GET /", http.FileServer(http.FS(webRoot)))

	server := &http.Server{Addr: *addr, Handler: browserSecurityHeaders(mux), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	log.Printf("deviceauth demo listening at http://%s", *addr)
	log.Printf("create user-bound registration invites with POST /admin/invites")
	log.Printf("restart clears demo accounts, device keys, challenges, and sessions")
	log.Fatal(server.ListenAndServe())
}

func browserSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'self'; frame-ancestors 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Frame-Options", "DENY")
		next.ServeHTTP(w, r)
	})
}

func validUserID(id string) bool {
	if len(id) < 3 || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.' || c == '@') {
			return false
		}
	}
	return true
}

func analyzeRegistrationRequest(userID, email string, accounts *accountStore) string {
	if !validUserID(userID) {
		return "Account ID must be 3–128 letters, digits, or . _ - @."
	}
	address, err := mail.ParseAddress(email)
	if err != nil || !strings.EqualFold(address.Address, email) {
		return "Enter a valid email address."
	}
	if accounts.exists(userID) {
		return "An account with this ID is already registered."
	}
	return ""
}

func randomIDOrFatal(size int) string {
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		log.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(value)
}

func constantTimeEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := range a {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}

func readJSON(w http.ResponseWriter, r *http.Request, out any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request")
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(w, http.StatusBadRequest, "request must contain one JSON value")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
