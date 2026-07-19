//go:build !windows

package fh

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type preforkHealthResponse struct {
	PID   int    `json:"pid"`
	Index string `json:"index"`
	Gen   string `json:"gen"`
}

func buildPreforkExampleBinary(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "prefork-example")
	cmd := exec.Command("go", "build", "-race", "-o", bin, "./examples/prefork")
	cmd.Dir = "."
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("building examples/prefork: %v\n%s", err, out)
	}
	return bin
}

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func fetchPreforkHealth(client *http.Client, url string) (preforkHealthResponse, error) {
	resp, err := client.Get(url)
	if err != nil {
		return preforkHealthResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return preforkHealthResponse{}, fmt.Errorf("status %d", resp.StatusCode)
	}
	var h preforkHealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		return preforkHealthResponse{}, err
	}
	return h, nil
}

// TestPreforkZeroDowntimeRollingRestart proves the actual claim behind
// ListenPrefork: it builds the examples/prefork binary, runs it as a real OS
// process (the master), hammers its /health endpoint continuously from a
// background goroutine, sends it a real SIGHUP mid-traffic, and asserts the
// port never stops accepting connections while the worker generation visibly
// rolls over — then sends SIGTERM and confirms a clean shutdown. The binary
// is built with -race so any concurrency bug in the supervisor itself (not
// just in this test) would be caught here too.
func TestPreforkZeroDowntimeRollingRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns real OS processes and sleeps through a rolling restart; skipped in -short mode")
	}

	bin := buildPreforkExampleBinary(t)
	addr := freeTCPAddr(t)
	healthURL := "http://" + addr + "/health"

	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), "PREFORK_ADDR="+addr, "PREFORK_WORKERS=2")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start master: %v", err)
	}
	masterDone := make(chan error, 1)
	go func() { masterDone <- cmd.Wait() }()
	// handledShutdown is set once the test body itself performs the graceful
	// SIGTERM-and-wait at the end of the happy path; Cleanup only needs to
	// act as a fallback for a test that fails earlier. It is only written
	// and read sequentially (Cleanup funcs run after the test body returns),
	// so no synchronization is needed.
	handledShutdown := false
	t.Cleanup(func() {
		if handledShutdown {
			return
		}
		// Prefer a graceful SIGTERM so the master reaps and kills its own
		// worker processes (see preforkSupervisor.shutdown) instead of
		// leaving them orphaned; only SIGKILL the master itself as a last
		// resort, and always bound every wait so a broken master can never
		// hang the test suite.
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-masterDone:
			return
		case <-time.After(5 * time.Second):
		}
		_ = cmd.Process.Kill()
		select {
		case <-masterDone:
		case <-time.After(2 * time.Second):
		}
	})

	client := &http.Client{Timeout: 2 * time.Second}

	// Wait for the initial generation to come up.
	deadline := time.Now().Add(10 * time.Second)
	var firstGen string
	for {
		if time.Now().After(deadline) {
			t.Fatal("server never became healthy")
		}
		h, err := fetchPreforkHealth(client, healthURL)
		if err == nil {
			firstGen = h.Gen
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Hammer /health continuously through the whole rollout window.
	var total, failed atomic.Int64
	stopPolling := make(chan struct{})
	pollerDone := make(chan struct{})
	go func() {
		defer close(pollerDone)
		for {
			select {
			case <-stopPolling:
				return
			default:
			}
			total.Add(1)
			if _, err := fetchPreforkHealth(client, healthURL); err != nil {
				failed.Add(1)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	time.Sleep(150 * time.Millisecond) // let steady traffic establish

	if err := cmd.Process.Signal(syscall.SIGHUP); err != nil {
		close(stopPolling)
		<-pollerDone
		t.Fatalf("SIGHUP master: %v", err)
	}

	// Wait for the generation to roll over, while traffic keeps flowing.
	rolledOver := false
	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		h, err := fetchPreforkHealth(client, healthURL)
		if err == nil && h.Gen != firstGen && h.Gen != "" {
			rolledOver = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !rolledOver {
		close(stopPolling)
		<-pollerDone
		t.Fatal("worker generation never rolled over after SIGHUP")
	}

	time.Sleep(150 * time.Millisecond) // a little more steady traffic post-rollout

	close(stopPolling)
	<-pollerDone

	if got := failed.Load(); got != 0 {
		t.Fatalf("%d/%d /health requests failed during the rolling restart; want 0", got, total.Load())
	}
	if total.Load() == 0 {
		t.Fatal("poller never issued a request")
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM master: %v", err)
	}
	select {
	case err := <-masterDone:
		handledShutdown = true
		if err != nil {
			t.Fatalf("master exited with error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("master did not exit after SIGTERM")
	}
}
