package etag

import "testing"

func TestMatch(t *testing.T) {
	if !match(`"a", "b"`, `"b"`) {
		t.Fatal("expected match")
	}
}
