package cluster

import (
	"context"
	"crypto/tls"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func TestForwardingRequiresLeaseAndAuthenticatedNode(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	lease, err := store.Acquire(ctx, "tenant/dev", "node-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	f := &AuthenticatedForwarder{NodeID: "node-b", Store: store, Peers: map[string]string{"node-a": "https://node-a.example"}, HandlerFunc: func(_ context.Context, id, op string, p []byte) ([]byte, error) {
		return append([]byte("ok:"), p...), nil
	}}
	req := httptest.NewRequest("POST", "https://node-b/mira/forward", nil)
	req.Header.Set("X-MIRA-Source", "node-a")
	req.Header.Set("X-MIRA-Identity", lease.Identity)
	req.Header.Set("X-MIRA-Fence", strconv.FormatUint(lease.Fence, 10))
	req.Header.Set("X-MIRA-Operation", "12345678")
	req.TLS = &tls.ConnectionState{}
	res := httptest.NewRecorder()
	f.Handler().ServeHTTP(res, req)
	if res.Code != 401 {
		t.Fatalf("mTLS bypass accepted: %d", res.Code)
	}
	req.TLS.PeerCertificates = nil
	// Explicitly require CA-verification, even if a certificate was presented.
	res = httptest.NewRecorder()
	f.Handler().ServeHTTP(res, req)
	if res.Code != 401 {
		t.Fatal("unauthenticated forward")
	}
}
