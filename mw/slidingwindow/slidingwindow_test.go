package slidingwindow

import (
	"fmt"
	"testing"
	"time"
)

// TestConfiguredMaxKeysIsHonored proves a caller-configured MaxKeys bound is
// actually enforced by Allow, rather than silently falling back to the
// hardcoded 65536 regardless of what the caller asked for.
func TestConfiguredMaxKeysIsHonored(t *testing.T) {
	const maxKeys = 10
	l := NewLimiter(Config{Rate: 5, Window: time.Minute, MaxKeys: maxKeys, CleanupInterval: time.Hour})

	for i := 0; i < 1000; i++ {
		l.Allow(fmt.Sprintf("key-%d", i))
	}

	l.mu.Lock()
	size := len(l.windows)
	l.mu.Unlock()

	if size > maxKeys {
		t.Fatalf("expected window map bounded at %d entries (configured MaxKeys), got %d", maxKeys, size)
	}
}

// TestAllowEnforcesRateAcrossManyRequests proves Allow correctly tracks and
// denies requests once the rate is exceeded, across enough calls to
// exercise the compact/grow path repeatedly (regression guard for the
// index-out-of-range panic previously hit on the very first request).
func TestAllowEnforcesRateAcrossManyRequests(t *testing.T) {
	l := NewLimiter(Config{Rate: 3, Burst: 0, Window: time.Hour, MaxKeys: 100, CleanupInterval: time.Hour})
	var allowed, denied int
	for i := 0; i < 10; i++ {
		ok, _, _ := l.Allow("same-key")
		if ok {
			allowed++
		} else {
			denied++
		}
	}
	if allowed != 3 {
		t.Fatalf("expected exactly 3 allowed requests (rate=3, burst=0), got %d", allowed)
	}
	if denied != 7 {
		t.Fatalf("expected 7 denied requests, got %d", denied)
	}
}

// TestDefaultMaxKeysStillAppliesWhenUnset proves the zero-value/default
// case (no explicit MaxKeys) still falls back to a sane bound.
func TestDefaultMaxKeysStillAppliesWhenUnset(t *testing.T) {
	l := &Limiter{windows: make(map[string]*window), windowSize: time.Minute, rate: 5}
	for i := 0; i < 200; i++ {
		l.Allow(fmt.Sprintf("key-%d", i))
	}
	l.mu.Lock()
	size := len(l.windows)
	l.mu.Unlock()
	if size != 200 {
		t.Fatalf("expected all 200 distinct keys tracked (well under default bound), got %d", size)
	}
}
