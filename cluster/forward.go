package cluster

// AuthenticatedForwarder is an mTLS-only inter-node request plane. It does
// not transfer live UDP cipher state. A failed node loses its fenced lease;
// clients establish a new TLS-authenticated MIRA session and resume durable
// application events from a shared transactional outbox.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

const maxForward = 1 << 20

type ForwardHandler func(context.Context, string, string, []byte) ([]byte, error)
type AuthenticatedForwarder struct {
	NodeID      string
	Store       LeaseStore
	HandlerFunc ForwardHandler
	Peers       map[string]string // node ID -> HTTPS endpoint URL
	Client      *http.Client      // MUST use client certificate and verified trusted roots
}

func (f *AuthenticatedForwarder) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != "POST" || r.URL.Path != "/mira/forward" {
			http.NotFound(w, r)
			return
		}
		if f.NodeID == "" || f.Store == nil || f.HandlerFunc == nil {
			http.Error(w, "not configured", http.StatusServiceUnavailable)
			return
		}
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 {
			http.Error(w, "mutual TLS required", http.StatusUnauthorized)
			return
		}
		source := r.Header.Get("X-MIRA-Source")
		if source == "" || source != r.TLS.PeerCertificates[0].Subject.CommonName {
			http.Error(w, "untrusted forwarding identity", http.StatusForbidden)
			return
		}
		if _, ok := f.Peers[source]; !ok {
			http.Error(w, "unknown node", http.StatusForbidden)
			return
		}
		identity := r.Header.Get("X-MIRA-Identity")
		operation := r.Header.Get("X-MIRA-Operation")
		if identity == "" || len(identity) > 256 || len(operation) < 8 || len(operation) > 256 {
			http.Error(w, "invalid routing metadata", http.StatusBadRequest)
			return
		}
		fence, err := strconv.ParseUint(r.Header.Get("X-MIRA-Fence"), 10, 64)
		if err != nil || fence == 0 {
			http.Error(w, "invalid fence", http.StatusBadRequest)
			return
		}
		lease, err := f.Store.Current(r.Context(), identity)
		if err != nil || lease.Owner != f.NodeID || lease.Fence != fence {
			http.Error(w, "ownership changed", http.StatusConflict)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxForward)
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "oversized forwarding request", http.StatusRequestEntityTooLarge)
			return
		}
		out, err := f.HandlerFunc(r.Context(), identity, operation, payload)
		if err != nil {
			http.Error(w, "forwarded operation failed", http.StatusBadGateway)
			return
		}
		if len(out) > maxForward {
			http.Error(w, "oversized response", http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(out)
	})
}
func (f *AuthenticatedForwarder) Forward(ctx context.Context, toNode string, lease Lease, operationID string, data []byte) ([]byte, error) {
	if f.Client == nil || f.Client.Transport == nil || f.NodeID == "" || len(operationID) < 8 || len(data) > maxForward {
		return nil, ErrInvalid
	}
	url, ok := f.Peers[toNode]
	if !ok || toNode != lease.Owner {
		return nil, ErrFenced
	}
	if len(url) < 8 || url[:8] != "https://" {
		return nil, errors.New("cluster: forwarding requires HTTPS")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", url+"/mira/forward", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-MIRA-Source", f.NodeID)
	req.Header.Set("X-MIRA-Identity", lease.Identity)
	req.Header.Set("X-MIRA-Fence", strconv.FormatUint(lease.Fence, 10))
	req.Header.Set("X-MIRA-Operation", operationID)
	resp, err := f.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("cluster: remote status %d", resp.StatusCode)
	}
	result, err := io.ReadAll(io.LimitReader(resp.Body, maxForward+1))
	if err != nil {
		return nil, err
	}
	if len(result) > maxForward {
		return nil, errors.New("cluster: oversize response")
	}
	return result, nil
}
