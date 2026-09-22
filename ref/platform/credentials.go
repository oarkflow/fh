package platform

import (
	"github.com/oarkflow/fh/ref/invocation"
)

// Credentials cross one boundary in two directions.
//
// REF's own invocation carries a PrincipalHint, which is what transports fill
// in. The SPI's Credentials is what authenticator adapters consume, and it
// carries a little more (basic-auth username and password, the raw headers, the
// remote address and whether the connection was TLS) because real
// authenticators need it.
//
// These two functions are the only place the shapes are converted, so an
// adapter never has to know about invocation, and the transport layer never has
// to know about the SPI.

// credentialsFrom projects a REF invocation onto SPI credentials.
func credentialsFrom(inv *invocation.Invocation) Credentials {
	if inv == nil {
		return Credentials{}
	}
	hint := inv.Principal
	creds := Credentials{
		BearerToken: hint.BearerToken,
		APIKey:      hint.APIKey,
		SessionID:   hint.SessionID,
		RemoteIP:    inv.Transport.RemoteIP,
		TLS:         inv.Transport.TLS,
	}
	switch meta := inv.Metadata.(type) {
	case invocation.HTTPMeta:
		creds.Headers = flattenHeaders(meta.Headers)
		if username, password, ok := basicCredentials(headerValue(meta.Headers, "Authorization")); ok {
			creds.Username, creds.Password = username, password
		}
	case invocation.QueueMeta:
		creds.Headers = meta.Headers
	case invocation.GRPCMeta:
		creds.Headers = flattenHeaders(meta.Metadata)
	}
	return creds
}

// credentialsToHint projects SPI credentials back onto a REF PrincipalHint, for
// host code written against the older LegacyAuthenticator signature.
func credentialsToHint(creds Credentials) invocation.PrincipalHint {
	return invocation.NewPrincipalHint(creds.BearerToken, creds.APIKey, nil, creds.SessionID)
}

// headerValue reads one header case-insensitively. Go's own HTTP server
// canonicalises header names, but a queue or gRPC transport may not, so every
// read goes through this rather than assuming a particular casing.
func headerValue(headers map[string][]string, name string) string {
	if values, ok := headers[name]; ok && len(values) > 0 {
		return values[0]
	}
	for key, values := range headers {
		if len(values) > 0 && equalFoldASCII(key, name) {
			return values[0]
		}
	}
	return ""
}

// flattenHeaders keeps the first value of each header. Authentication never
// depends on a repeated header, and collapsing them keeps the credential shape
// simple for adapter authors.
func flattenHeaders(headers map[string][]string) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	out := make(map[string]string, len(headers))
	for key, values := range headers {
		if len(values) > 0 {
			out[key] = values[0]
		}
	}
	return out
}

// basicCredentials decodes an HTTP basic Authorization header. It is here rather
// than in the basic authenticator because the header is parsed once per request
// and shared by every link of an auth.chain.
func basicCredentials(header string) (string, string, bool) {
	const prefix = "Basic "
	if len(header) <= len(prefix) || !equalFoldASCII(header[:len(prefix)], prefix) {
		return "", "", false
	}
	decoded, err := base64Std.DecodeString(header[len(prefix):])
	if err != nil {
		return "", "", false
	}
	for i, b := range decoded {
		if b == ':' {
			return string(decoded[:i]), string(decoded[i+1:]), true
		}
	}
	return "", "", false
}

// equalFoldASCII compares two ASCII strings case-insensitively without the
// allocation strings.EqualFold's Unicode handling can involve. Header prefixes
// are always ASCII.
func equalFoldASCII(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if 'A' <= ca && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if 'A' <= cb && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}
