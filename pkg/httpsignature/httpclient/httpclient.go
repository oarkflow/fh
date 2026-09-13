// Package httpclient provides optional net/http convenience wrappers around
// github.com/oarkflow/fh/pkg/httpsignature's RFC 9421 response-signature
// verification.
//
// It is kept out of the core httpsignature package deliberately: callers
// that already have the response fields extracted (most notably a WASM
// browser client using the Fetch API instead of net/http) can call
// httpsignature.Verifier.VerifyMessage directly and never import net/http at
// all. Merely importing net/http — even unused — pulls in its dependency
// graph (crypto/tls, crypto/x509, compress/gzip, and more) and adds several
// megabytes to a compiled Go/WASM binary. Use this package only when you
// have a real *http.Request/*http.Response pair from net/http.
package httpclient

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/oarkflow/fh/pkg/httpsignature"
)

// Verify checks a native net/http response against the given RFC 9421
// verifier profile.
func Verify(v httpsignature.Verifier, request *http.Request, response *http.Response, body []byte, expectedNonce string) error {
	if request == nil || response == nil || request.URL == nil {
		return httpsignature.ErrPolicy
	}
	return v.VerifyMessage(request.Method, request.URL.String(), httpsignature.ResponseMessage{
		Status:         response.StatusCode,
		ContentDigest:  joinedHeader(response.Header, httpsignature.HeaderContentDigest),
		ContentType:    joinedHeader(response.Header, "Content-Type"),
		SignatureInput: joinedHeader(response.Header, httpsignature.HeaderSignatureInput),
		Signature:      joinedHeader(response.Header, httpsignature.HeaderSignature),
	}, body, expectedNonce)
}

// Client wraps an *http.Client, verifying each response's RFC 9421 signature
// against a fresh nonce before returning it to the caller.
type Client struct {
	HTTPClient  *http.Client
	Verifier    httpsignature.Verifier
	MaxBodySize int64
}

// Do requests a fresh nonce-bound RFC 9421 response signature, reads and
// verifies the exact response content, then restores Body for the caller.
func (c Client) Do(request *http.Request) (*http.Response, error) {
	if request == nil || request.URL == nil {
		return nil, httpsignature.ErrPolicy
	}
	nonce, err := httpsignature.NewNonce()
	if err != nil {
		return nil, err
	}
	accept, err := httpsignature.FormatAcceptSignature(c.Verifier.Label, nonce, c.Verifier.KeyID)
	if err != nil {
		return nil, err
	}
	request.Header.Set(httpsignature.HeaderAcceptSignature, accept)
	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	limit := c.MaxBodySize
	if limit <= 0 {
		limit = 16 << 20
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, limit+1))
	closeErr := response.Body.Close()
	if readErr != nil {
		return nil, readErr
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("%w: response exceeds %d bytes", httpsignature.ErrPolicy, limit)
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if err := Verify(c.Verifier, request, response, body, nonce); err != nil {
		return nil, err
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	response.ContentLength = int64(len(body))
	return response, nil
}

func joinedHeader(header http.Header, name string) string {
	values := header.Values(name)
	for i := range values {
		values[i] = strings.TrimSpace(values[i])
	}
	return strings.Join(values, ", ")
}
