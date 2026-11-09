package scheduler

import (
	"bytes"
	"testing"
)

func TestCoalesce(t *testing.T) {
	q := NewLatestQueue(2)
	if !q.Put("a", []byte("old")) || !q.Put("b", []byte("b")) || !q.Put("a", []byte("new")) {
		t.Fatal("put")
	}
	if q.Put("c", nil) {
		t.Fatal("overflow accepted")
	}
	key, got, ok := q.Pop()
	if !ok || key != "a" || !bytes.Equal(got, []byte("new")) {
		t.Fatalf("bad pop %s:%q", key, got)
	}
	if q.Len() != 1 {
		t.Fatal("wrong length")
	}
}
