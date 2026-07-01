package tracing

import "testing"

func TestParse(t *testing.T) {
	tr, sp := parse("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
	if tr == "" || sp == "" {
		t.Fatal("parse failed")
	}
}
