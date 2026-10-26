package fh

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func limitedTestResponse(body string, limit int64) *Response {
	return &Response{Raw: &http.Response{Body: io.NopCloser(strings.NewReader(body))}, limit: limit}
}

func TestResponseLimitAcrossReadPaths(t *testing.T) {
	for _, body := range []string{"abc", "abcd"} {
		wantErr := len(body) > 3
		r := limitedTestResponse(body, 3)
		got, err := io.ReadAll(r.Reader())
		if string(got) != body[:min(len(body), 3)] || (errors.Is(err, ErrClientBodyTooLarge) != wantErr) {
			t.Fatalf("Reader(%q) = %q, %v", body, got, err)
		}
		r = limitedTestResponse(body, 3)
		var out bytes.Buffer
		n, err := r.DownloadTo(&out)
		if n != int64(out.Len()) || out.String() != body[:min(len(body), 3)] || (errors.Is(err, ErrClientBodyTooLarge) != wantErr) {
			t.Fatalf("DownloadTo(%q) = %d, %q, %v", body, n, out.String(), err)
		}
		r = limitedTestResponse(body, 3)
		got, err = r.Bytes()
		if wantErr {
			if !errors.Is(err, ErrClientBodyTooLarge) || got != nil {
				t.Fatalf("Bytes(%q) = %q, %v", body, got, err)
			}
		} else if err != nil || string(got) != body {
			t.Fatalf("Bytes(%q) = %q, %v", body, got, err)
		}
	}
}

func TestSaveAtomicKeepsDestinationOnLimitError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "download.txt")
	if err := os.WriteFile(path, []byte("previous"), 0600); err != nil {
		t.Fatal(err)
	}
	err := limitedTestResponse("excess", 3).SaveAtomic(path, 0600)
	got, readErr := os.ReadFile(path)
	if !errors.Is(err, ErrClientBodyTooLarge) || readErr != nil || string(got) != "previous" {
		t.Fatalf("destination = %q, download err=%v read err=%v", got, err, readErr)
	}
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".fh-download-*"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("temporary files = %v, %v", matches, err)
	}
}
