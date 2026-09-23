package http

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/oarkflow/fh"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type RolloutMode string

const (
	RolloutLegacy RolloutMode = "legacy"
	RolloutShadow RolloutMode = "shadow"
	RolloutCanary RolloutMode = "canary"
)

type RequestSnapshot struct {
	Method, URL, Host, RemoteAddr, RequestID string
	TLS                                      bool
	Header                                   http.Header
	Body                                     []byte
}
type ResponseSnapshot struct {
	Status int
	Header http.Header
	Body   []byte
}
type PreviewFunc func(context.Context, RequestSnapshot) (ResponseSnapshot, error)
type Difference struct {
	RequestID                     string
	LegacyStatus, CandidateStatus int
	LegacyDigest, CandidateDigest string
}
type RolloutOptions struct {
	Mode                 RolloutMode
	Legacy               http.Handler
	Candidate            http.Handler
	Preview              PreviewFunc
	CanaryPercent        float64
	MaxBodyBytes         int64
	OnDifference         func(Difference)
	OnPreviewError       func(error)
	MaxShadowConcurrency int
}

// NewRollout builds a JSON/API migration handler. Shadow mode always serves the
// legacy response and sends only an immutable snapshot to Preview. Preview must
// call a read-only engine path; arbitrary Go handlers cannot be proven pure.
func NewRollout(opts RolloutOptions) (http.Handler, error) {
	if opts.Legacy == nil {
		return nil, fmt.Errorf("http rollout: legacy handler is required")
	}
	if opts.Mode == "" {
		opts.Mode = RolloutLegacy
	}
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = 1 << 20
	}
	if opts.MaxShadowConcurrency <= 0 {
		opts.MaxShadowConcurrency = 16
	}
	shadowSlots := make(chan struct{}, opts.MaxShadowConcurrency)
	if opts.Mode != RolloutLegacy && opts.Candidate == nil && opts.Preview == nil {
		return nil, fmt.Errorf("http rollout: candidate or preview is required")
	}
	if opts.Mode == RolloutShadow && opts.Preview == nil {
		return nil, fmt.Errorf("http rollout: shadow mode requires a read-only preview function")
	}
	if opts.Mode == RolloutCanary && opts.Candidate == nil {
		return nil, fmt.Errorf("http rollout: canary mode requires a candidate handler")
	}
	if opts.CanaryPercent < 0 || opts.CanaryPercent > 100 {
		return nil, fmt.Errorf("http rollout: canary percentage must be between 0 and 100")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body == nil {
			r.Body = http.NoBody
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, opts.MaxBodyBytes+1))
		if err != nil {
			http.Error(w, "invalid request body", 400)
			return
		}
		if int64(len(body)) > opts.MaxBodyBytes {
			http.Error(w, "request body too large", 413)
			return
		}
		id := rolloutRequestID(r)
		snapshotHeaders := r.Header.Clone()
		snapshotHeaders.Set("X-Request-ID", id)
		snapshot := RequestSnapshot{Method: r.Method, URL: r.URL.RequestURI(), Host: r.Host, TLS: r.TLS != nil, RemoteAddr: r.RemoteAddr, RequestID: id, Header: snapshotHeaders, Body: bytes.Clone(body)}
		clone := func() *http.Request {
			c := r.Clone(r.Context())
			c.Body = io.NopCloser(bytes.NewReader(bytes.Clone(body)))
			c.ContentLength = int64(len(body))
			c.Header.Set("X-Request-ID", id)
			return c
		}
		w.Header().Set("X-Request-ID", id)
		switch opts.Mode {
		case RolloutLegacy:
			opts.Legacy.ServeHTTP(w, clone())
		case RolloutCanary:
			if selectedCanary(stableCanaryKey(r.Header.Get("Authorization")+"\x00"+r.Header.Get("X-API-Key")+"\x00"+r.Header.Get("Cookie")+"\x00"+r.RemoteAddr, r.URL.Path), r.URL.Path, opts.CanaryPercent) {
				opts.Candidate.ServeHTTP(w, clone())
			} else {
				opts.Legacy.ServeHTTP(w, clone())
			}
		case RolloutShadow:
			recorder := newCaptureWriter()
			opts.Legacy.ServeHTTP(recorder, clone())
			legacy := recorder.snapshot()
			recorder.copyTo(w)
			if legacy.Status < 200 || legacy.Status >= 300 { return }
			select {
			case shadowSlots <- struct{}{}:
				go func(snapshot RequestSnapshot, legacy ResponseSnapshot) {
					defer func() { <-shadowSlots }()
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					defer cancel()
					candidate, previewErr := opts.Preview(ctx, snapshot)
					if previewErr != nil {
						if opts.OnPreviewError != nil {
							opts.OnPreviewError(previewErr)
						}
						return
					}
					if opts.OnDifference != nil && !sameResponse(legacy, candidate) {
						opts.OnDifference(Difference{RequestID: id, LegacyStatus: legacy.Status, CandidateStatus: candidate.Status, LegacyDigest: digest(legacy.Body), CandidateDigest: digest(candidate.Body)})
					}
				}(snapshot, legacy)
			default: // Skip shadow work when saturated.
			}
		default:
			http.Error(w, "invalid rollout mode", 500)
		}
	}), nil
}

type captureWriter struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func newCaptureWriter() *captureWriter       { return &captureWriter{header: make(http.Header)} }
func (w *captureWriter) Header() http.Header { return w.header }
func (w *captureWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
}
func (w *captureWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	return w.body.Write(p)
}
func (w *captureWriter) snapshot() ResponseSnapshot {
	status := w.status
	if status == 0 {
		status = 200
	}
	return ResponseSnapshot{Status: status, Header: w.header.Clone(), Body: bytes.Clone(w.body.Bytes())}
}
func (w *captureWriter) copyTo(dst http.ResponseWriter) {
	for k, vs := range w.header {
		dst.Header()[k] = append([]string(nil), vs...)
	}
	if w.status != 0 {
		dst.WriteHeader(w.status)
	}
	_, _ = dst.Write(w.body.Bytes())
}
func sameResponse(a, b ResponseSnapshot) bool {
	if a.Status != b.Status {
		return false
	}
	canon := func(raw []byte) []byte {
		var v any
		if json.Unmarshal(raw, &v) == nil {
			if b, e := json.Marshal(v); e == nil {
				return b
			}
		}
		return bytes.TrimSpace(raw)
	}
	return bytes.Equal(canon(a.Body), canon(b.Body))
}
func digest(body []byte) string { h := sha256.Sum256(body); return hex.EncodeToString(h[:8]) }
func rolloutRequestID(r *http.Request) string {
	if v := r.Header.Get("X-Request-ID"); validID(v) {
		return v
	}
	return newRequestID()
}
func validID(v string) bool {
	if v == "" || len(v) > 128 {
		return false
	}
	for _, r := range v {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("-_.:", r)) {
			return false
		}
	}
	return true
}
func stableCanaryKey(identity, path string) string {
	h := sha256.Sum256([]byte(identity + " " + path))
	return hex.EncodeToString(h[:])
}
func selectedCanary(id, path string, pct float64) bool {
	if pct <= 0 {
		return false
	}
	if pct >= 100 {
		return true
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(id + " " + path))
	return float64(h.Sum32()%10000)/100 < pct
}

// SnapshotQuery parses query values for preview adapters without losing repeats.
func SnapshotQuery(snapshot RequestSnapshot) map[string][]string {
	u, err := url.Parse(snapshot.URL)
	if err != nil {
		return nil
	}
	q := u.Query()
	out := make(map[string][]string, len(q))
	for k, v := range q {
		out[k] = append([]string(nil), v...)
	}
	return out
}

// CanaryFH returns an fh handler that deterministically sends a percentage of
// requests to candidate while keeping the rest on legacy.
func CanaryFH(legacy, candidate fh.HandlerFunc, percent float64) (fh.HandlerFunc, error) {
	if legacy == nil || candidate == nil {
		return nil, fmt.Errorf("http rollout: both fh handlers are required")
	}
	if percent < 0 || percent > 100 {
		return nil, fmt.Errorf("http rollout: canary percentage must be between 0 and 100")
	}
	return func(c fh.Ctx) error {
		id := rolloutRequestIDFromFH(c)
		c.Set("X-Request-ID", id)
		if selectedCanary(stableCanaryKey(c.Get("Authorization")+"\x00"+c.Get("X-API-Key")+"\x00"+c.Get("Cookie")+"\x00"+c.IP(), c.Path()), c.Path(), percent) {
			return candidate(c)
		}
		return legacy(c)
	}, nil
}

// ShadowFH leaves the legacy fh handler responsible for the live response and
// compares it with a read-only preview. CaptureResponseBody records the legacy
// output for comparison without changing what the client receives.
func ShadowFH(legacy fh.HandlerFunc, preview PreviewFunc, maxBodyBytes int, onDifference func(Difference), onPreviewError func(error)) (fh.HandlerFunc, error) {
	if legacy == nil || preview == nil {
		return nil, fmt.Errorf("http rollout: legacy handler and preview are required")
	}
	if maxBodyBytes <= 0 {
		maxBodyBytes = 1 << 20
	}
	shadowSlots := make(chan struct{}, 16)
	return func(c fh.Ctx) error {
		body := c.BodyCopy()
		if len(body) > maxBodyBytes {
			return c.Status(413).JSON(map[string]string{"error": "request body too large"})
		}
		id := rolloutRequestIDFromFH(c)
		c.Set("X-Request-ID", id)
		headers := make(http.Header)
		for k, v := range c.GetReqHeaders() {
			headers[k] = append([]string(nil), v...)
		}
		headers.Set("X-Request-ID", id)
		snapshot := RequestSnapshot{Method: c.Method(), URL: c.OriginalURL(), Host: c.Hostname(), RemoteAddr: c.IP(), RequestID: id, TLS: c.Protocol() == "https", Header: headers, Body: body}
		c.CaptureResponseBody()
		err := legacy(c)
		legacyResponse := ResponseSnapshot{Status: c.StatusCode(), Body: c.ResponseBody()}
		if legacyResponse.Status == 0 {
			legacyResponse.Status = 200
		}
		if legacyResponse.Status < 200 || legacyResponse.Status >= 300 { return err }
		select {
		case shadowSlots <- struct{}{}:
			go func(snapshot RequestSnapshot, legacyResult ResponseSnapshot) {
				defer func() { <-shadowSlots }()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				candidate, previewErr := preview(ctx, snapshot)
				if previewErr != nil {
					if onPreviewError != nil {
						onPreviewError(previewErr)
					}
					return
				}
				if onDifference != nil && !sameResponse(legacyResult, candidate) {
					onDifference(Difference{RequestID: id, LegacyStatus: legacyResult.Status, CandidateStatus: candidate.Status, LegacyDigest: digest(legacyResult.Body), CandidateDigest: digest(candidate.Body)})
				}
			}(snapshot, legacyResponse)
		default: // Skip candidate work when saturated.
		}
		return err
	}, nil
}

func rolloutRequestIDFromFH(c fh.Ctx) string {
	if id := validRequestID(c.Get("X-Request-ID")); id != "" {
		return id
	}
	return newRequestID()
}
