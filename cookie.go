package fh

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"
)

type SameSite int8

const (
	SameSiteLax SameSite = iota
	SameSiteStrict
	SameSiteNone
)

type Cookie struct {
	Name        string
	Value       string
	Path        string
	Domain      string
	MaxAge      int
	Expires     time.Time
	Secure      bool
	HttpOnly    bool
	SameSite    SameSite
	Partitioned bool
}

var ErrInvalidCookie = errors.New("fasthttp: invalid cookie")

// Valid checks RFC 6265 syntax and security invariants for cookie prefixes,
// SameSite=None, and partitioned cookies.
func (c *Cookie) Valid() error {
	if c == nil || !validToken([]byte(c.Name)) || c.Name == "" || !validCookieValue(c.Value) {
		return ErrInvalidCookie
	}
	if strings.ContainsAny(c.Path, ";\x00\r\n") || strings.ContainsAny(c.Domain, ";\x00\r\n") {
		return ErrInvalidCookie
	}
	if c.Domain != "" && !validCookieDomain(c.Domain) {
		return ErrInvalidCookie
	}
	if c.SameSite == SameSiteNone && !c.Secure {
		return ErrInvalidCookie
	}
	if c.Partitioned && !c.Secure {
		return ErrInvalidCookie
	}
	if strings.HasPrefix(c.Name, "__Secure-") && !c.Secure {
		return ErrInvalidCookie
	}
	if strings.HasPrefix(c.Name, "__Host-") && (!c.Secure || c.Path != "/" || c.Domain != "") {
		return ErrInvalidCookie
	}
	return nil
}

// String returns the Set-Cookie header value for this cookie.
func (c *Cookie) String() string {
	if c.Valid() != nil {
		return ""
	}
	var b strings.Builder
	b.Grow(len(c.Name) + len(c.Value) + 80)
	b.WriteString(c.Name)
	b.WriteByte('=')
	b.WriteString(c.Value)

	if c.Path != "" {
		b.WriteString("; Path=")
		b.WriteString(c.Path)
	}
	if c.Domain != "" {
		b.WriteString("; Domain=")
		b.WriteString(c.Domain)
	}
	if c.MaxAge != 0 {
		b.WriteString("; Max-Age=")
		if c.MaxAge < 0 {
			b.WriteByte('0')
		} else {
			b.WriteString(strconv.Itoa(c.MaxAge))
		}
	}
	if !c.Expires.IsZero() {
		b.WriteString("; Expires=")
		b.WriteString(c.Expires.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT"))
	}
	if c.HttpOnly {
		b.WriteString("; HttpOnly")
	}
	if c.Secure {
		b.WriteString("; Secure")
	}
	switch c.SameSite {
	case SameSiteStrict:
		b.WriteString("; SameSite=Strict")
	case SameSiteNone:
		b.WriteString("; SameSite=None")
	default:
		b.WriteString("; SameSite=Lax")
	}
	if c.Partitioned {
		b.WriteString("; Partitioned")
	}
	return b.String()
}

func validCookieValue(value string) bool {
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c < 0x21 || c > 0x7e || c == '"' || c == ',' || c == ';' || c == '\\' {
			return false
		}
	}
	return true
}

func validCookieDomain(domain string) bool {
	domain = strings.TrimPrefix(domain, ".")
	if domain == "" || len(domain) > 253 || domain[0] == '-' || domain[len(domain)-1] == '-' || domain[0] == '.' || domain[len(domain)-1] == '.' {
		return false
	}
	labelLen := 0
	for _, c := range domain {
		if c == '.' {
			if labelLen == 0 || labelLen > 63 {
				return false
			}
			labelLen = 0
			continue
		}
		if !(c == '.' || c == '-' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
			return false
		}
		labelLen++
	}
	return labelLen > 0 && labelLen <= 63
}

// Sign signs the cookie value with HMAC-SHA256.
// The signed format is "value.base64_url_hmac".
func (c *Cookie) Sign(secret []byte) {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(c.Value))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	c.Value = c.Value + "." + sig
}

// Verify checks the HMAC signature on a signed cookie value.
func (c *Cookie) Verify(secret []byte) bool {
	i := strings.LastIndexByte(c.Value, '.')
	if i < 0 {
		return false
	}
	value := c.Value[:i]
	encodedSig := c.Value[i+1:]
	sig, err := base64.RawURLEncoding.DecodeString(encodedSig)
	if err != nil {
		return false
	}
	if base64.RawURLEncoding.EncodeToString(sig) != encodedSig {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(value))
	return hmac.Equal(sig, mac.Sum(nil))
}

// ParseCookie parses a Cookie request header value into a name-value map.
func ParseCookie(header string) map[string]string {
	cookies := make(map[string]string)
	for {
		header = strings.TrimSpace(header)
		if header == "" {
			break
		}
		semi := strings.IndexByte(header, ';')
		var part string
		if semi < 0 {
			part = header
			header = ""
		} else {
			part = header[:semi]
			header = header[semi+1:]
		}
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		eq := strings.IndexByte(part, '=')
		if eq < 0 {
			continue
		}
		name := strings.TrimSpace(part[:eq])
		val := strings.TrimSpace(part[eq+1:])
		if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
			val = val[1 : len(val)-1]
		}
		if !validToken([]byte(name)) || !validCookieValue(val) {
			continue
		}
		if _, exists := cookies[name]; !exists {
			cookies[name] = val
		}
	}
	return cookies
}

// SetCookie adds a Set-Cookie header to the response.
func (c *Ctx) SetCookie(cookie *Cookie) {
	if cookie == nil || cookie.Valid() != nil {
		return
	}
	c.responseCookies = append(c.responseCookies, *cookie)
}

// GetCookie returns the value of a named cookie from the request.
func (c *Ctx) GetCookie(name string) string {
	if !validToken([]byte(name)) {
		return ""
	}
	for i := 0; i < c.Header.hcount; i++ {
		if !bytesEqualFold(c.Header.headers[i].Key, []byte("Cookie")) {
			continue
		}
		if value, ok := findCookie(b2s(c.Header.headers[i].Value), name); ok {
			return value
		}
	}
	return ""
}

func findCookie(header, want string) (string, bool) {
	for header != "" {
		semi := strings.IndexByte(header, ';')
		part := header
		if semi >= 0 {
			part, header = header[:semi], header[semi+1:]
		} else {
			header = ""
		}
		part = strings.TrimSpace(part)
		eq := strings.IndexByte(part, '=')
		if eq <= 0 || strings.TrimSpace(part[:eq]) != want {
			continue
		}
		value := strings.TrimSpace(part[eq+1:])
		if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
			value = value[1 : len(value)-1]
		}
		if validCookieValue(value) {
			return value, true
		}
		return "", false
	}
	return "", false
}

// DelCookie deletes a cookie by name (sets MaxAge=-1 with an expired date).
func (c *Ctx) DelCookie(name string) {
	c.SetCookie(&Cookie{
		Name:     name,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		Expires:  time.Unix(0, 0),
		HttpOnly: true,
		Secure:   true,
		SameSite: SameSiteLax,
	})
}
