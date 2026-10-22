package fh

import (
	"testing"
)

// TestDirectJSONBaseGrowOnDemand pins the documented New/NewFast behavior that
// a zero-capacity connection-owned write buffer is promoted to a reusable
// connection-sized buffer on first use. Without the promotion, every JSON
// response beyond the initial 512-byte temp reallocates and splits into a
// header+body writev, which halves throughput on multi-KiB JSON responses and
// doubles their p95 latency (observed 95k→29k→130k req/s across variants).
func TestDirectJSONBaseGrowOnDemand(t *testing.T) {
	c := &DefaultCtx{server: &App{cfg: defaultConfig}}
	var connBuf []byte // simulates connState.writeBuf created empty (WriteBufferSize zero)
	c.writeBuf = &connBuf

	base := c.directJSONBase()
	if len(base) != directJSONHeaderReserve {
		t.Fatalf("base len = %d, want %d", len(base), directJSONHeaderReserve)
	}
	if cap(base) < directJSONHeaderReserve {
		t.Fatalf("base cap = %d, need room for header reserve", cap(base))
	}
	if cap(connBuf) < directJSONInitialBuffer {
		t.Fatalf("connection buffer not promoted grow-on-demand: cap=%d, want >= %d", cap(connBuf), directJSONInitialBuffer)
	}
	if cap(connBuf) != growDirectJSONBufferBytes {
		t.Fatalf("promoted cap = %d, want %d", cap(connBuf), growDirectJSONBufferBytes)
	}

	// The promoted buffer must be reused by the next response instead of
	// allocating a fresh temp per request.
	second := c.directJSONBase()
	if &second[0] != &base[0] {
		t.Fatal("second directJSONBase returned a different backing array; promoted buffer was not reused")
	}
}

// TestDirectJSONBasePooledFallback covers the standalone/H2 contract: a nil
// writeBuf borrows from the shared pool and stays reclaimable.
func TestDirectJSONBasePooledFallback(t *testing.T) {
	c := &DefaultCtx{server: &App{cfg: defaultConfig}}
	base := c.directJSONBase()
	if !c.writeBufPooled {
		t.Fatal("borrowed base must remain pooled so releaseCtx returns it")
	}
	if cap(*c.writeBuf) < directJSONInitialBuffer || cap(base) > cap(*c.writeBuf) {
		t.Fatalf("pooled base cap mismatch: base=%d slot=%d", cap(base), cap(*c.writeBuf))
	}
	releaseCtx(c)
}
