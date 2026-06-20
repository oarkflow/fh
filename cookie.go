package fasthttp

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"strings"
	"time"
)

type SameSite int8

const (
	SameSiteLax    SameSite = iota
	SameSiteStrict
	SameSiteNone
)

type Cookie struct {
	Name     string
	Value    string
	Path     string
	Domain   string
	MaxAge   int
	Expires  time.Time
	Secure   bool
	HttpOnly bool
	SameSite SameSite
}

// String returns the Set-Cookie header value for this cookie.
func (c *Cookie) String() string {
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
		b.WriteString(strconv.Itoa(c.MaxAge))
	}
	if !c.Expires.IsZero() {
		b.WriteString("; Expires=")
		b.WriteString(c.Expires.Format(time.RFC1123))
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
	return b.String()
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
	sig, err := base64.RawURLEncoding.DecodeString(c.Value[i+1:])
	if err != nil {
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
		cookies[name] = val
	}
	return cookies
}

// SetCookie adds a Set-Cookie header to the response.
func (c *Ctx) SetCookie(cookie *Cookie) {
	c.responseCookies = append(c.responseCookies, *cookie)
}

// GetCookie returns the value of a named cookie from the request.
func (c *Ctx) GetCookie(name string) string {
	v := c.Header.PeekStr("Cookie")
	if v == "" {
		return ""
	}
	cookies := ParseCookie(v)
	return cookies[name]
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
