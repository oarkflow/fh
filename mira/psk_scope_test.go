package mira

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestInsecurePSKProfileRejectsPublicAndWildcard(t *testing.T) {
	key := bytes.Repeat([]byte{61}, 32)
	_, err := Listen("0.0.0.0:0", key, Options{})
	if err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("wildcard PSK listener permitted: %v", err)
	}
	_, err = ListenWithResolver("0.0.0.0:0", func(string) ([]byte, bool) { return key, true }, Options{})
	if err == nil {
		t.Fatal("wildcard PSK resolver permitted")
	}
	_, err = Dial(context.Background(), "198.51.100.10:7447", key, Options{})
	if err == nil {
		t.Fatal("public PSK dial permitted")
	}
}
