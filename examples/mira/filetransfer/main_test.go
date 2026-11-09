package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestUploadCommitsVerifiedFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := filepath.Join(t.TempDir(), "report.bin")
	data := make([]byte, 3<<20+7)
	rand.Read(data)
	if err := os.WriteFile(src, data, 0o600); err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir()
	var out bytes.Buffer
	if err := run(ctx, &out, src, dest); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "verified") {
		t.Fatalf("output: %s", out.String())
	}
	entries, _ := os.ReadDir(dest)
	if len(entries) != 1 || entries[0].Name() != "report.bin" {
		t.Fatalf("destination should hold exactly the committed file, got %v", entries)
	}
}
