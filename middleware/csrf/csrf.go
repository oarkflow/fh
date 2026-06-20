// Package csrf implements the signed-origin-aware double-submit cookie pattern.
package csrf

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"net/url"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

type Config struct {
	CookieName     string
	HeaderName     string
	CookiePath     string
	CookieDomain   string
	CookieSecure   bool
	CookieSameSite fh.SameSite
	CookieMaxAge   time.Duration
	TrustedOrigins []string
	Next           func(*fh.Ctx) bool
}

var DefaultConfig = Config{
	CookieName: "csrf_token", HeaderName: "X-CSRF-Token", CookiePath: "/",
	CookieSameSite: fh.SameSiteLax, CookieMaxAge: 12 * time.Hour,
}

func New(config ...Config) fh.HandlerFunc {
	cfg := DefaultConfig
	if len(config) > 0 {
		merge(&cfg, config[0])
	}
	trusted := make(map[string]struct{}, len(cfg.TrustedOrigins))
	for _, origin := range cfg.TrustedOrigins {
		trusted[strings.ToLower(strings.TrimRight(origin, "/"))] = struct{}{}
	}
	return func(c *fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}
		token := c.GetCookie(cfg.CookieName)
		if token == "" {
			var err error
			token, err = newToken()
			if err != nil {
				return fh.InternalError(err)
			}
			c.SetCookie(&fh.Cookie{Name: cfg.CookieName, Value: token, Path: cfg.CookiePath, Domain: cfg.CookieDomain, MaxAge: int(cfg.CookieMaxAge.Seconds()), Secure: cfg.CookieSecure, HttpOnly: false, SameSite: cfg.CookieSameSite})
		}
		c.Locals("csrf_token", token)
		if safeMethod(c.Method()) {
			return c.Next()
		}
		if !validOrigin(c, trusted) {
			return csrfError("Request origin is not allowed")
		}
		provided := c.Get(cfg.HeaderName)
		if provided == "" || len(provided) != len(token) || subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			return csrfError("CSRF token is missing or invalid")
		}
		return c.Next()
	}
}

func merge(dst *Config, src Config) {
	if src.CookieName != "" {
		dst.CookieName = src.CookieName
	}
	if src.HeaderName != "" {
		dst.HeaderName = src.HeaderName
	}
	if src.CookiePath != "" {
		dst.CookiePath = src.CookiePath
	}
	if src.CookieDomain != "" {
		dst.CookieDomain = src.CookieDomain
	}
	if src.CookieMaxAge > 0 {
		dst.CookieMaxAge = src.CookieMaxAge
	}
	if src.TrustedOrigins != nil {
		dst.TrustedOrigins = src.TrustedOrigins
	}
	dst.CookieSecure, dst.CookieSameSite, dst.Next = src.CookieSecure, src.CookieSameSite, src.Next
}

func safeMethod(method string) bool {
	return method == "GET" || method == "HEAD" || method == "OPTIONS" || method == "TRACE"
}
func newToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}
func csrfError(message string) *fh.HTTPError {
	return fh.NewHTTPError(fh.StatusForbidden, "CSRF_INVALID", message)
}

func validOrigin(c *fh.Ctx, trusted map[string]struct{}) bool {
	raw := c.Get("Origin")
	if raw == "" {
		raw = c.Get("Referer")
	}
	if raw == "" {
		return true
	} // Non-browser clients; the token remains required.
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return false
	}
	origin := strings.ToLower(u.Scheme + "://" + u.Host)
	if _, ok := trusted[origin]; ok {
		return true
	}
	return strings.EqualFold(u.Host, string(c.Header.Host))
}
