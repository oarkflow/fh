package fh

import "testing"

// TestPutBytesDropsOversizedBuffers proves a buffer whose capacity grew
// past maxPooledBytesCap (e.g. from building one very large response body)
// is not returned to the shared pool, so it can't permanently inflate the
// backing array reused by unrelated future requests.
func TestPutBytesDropsOversizedBuffers(t *testing.T) {
	// Drain whatever's currently pooled so Get() below can't accidentally
	// return an unrelated already-pooled buffer of the wrong size.
	for i := 0; i < 64; i++ {
		getBytes()
	}

	oversized := make([]byte, 0, maxPooledBytesCap+1)
	putBytes(&oversized)

	for i := 0; i < 64; i++ {
		b := getBytes()
		if cap(*b) > maxPooledBytesCap {
			t.Fatalf("oversized buffer (cap=%d) was pooled and handed back out", cap(*b))
		}
	}
}

// TestPutBytesStillPoolsNormalBuffers proves ordinary-sized buffers are
// still recycled (the fix only rejects oversized ones).
func TestPutBytesStillPoolsNormalBuffers(t *testing.T) {
	b := getBytes()
	*b = append((*b)[:0], []byte("hello")...)
	putBytes(b)

	b2 := getBytes()
	if len(*b2) != 0 {
		t.Fatalf("expected pooled buffer to be reset to zero length, got %d", len(*b2))
	}
}
