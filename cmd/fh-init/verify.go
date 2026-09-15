package main

// verify.go implements fh-init's opt-in "-verify" phase: it builds the
// generated project's frontend and server, boots the server on a loopback
// ephemeral port using its own development credentials, exercises the exact
// HTTP surface a browser depends on (static assets, the WASM transport
// bundle, and the full login / secure-bootstrap / logout lifecycle), and
// opens the system's default browser against the running instance for a
// live look before shutting it back down.
//
// Nothing under web/public is checked into the template - it is Node build
// output (npm install && npm run build; see web/frontend/README.md), not
// something fh-init's -module/-dir scaffold ever builds or embeds itself.
// -verify is the one place a build happens, because "verify the generated
// app" is meaningless without it: it needs network access for both `npm
// install` and `go mod tidy`, so a missing network, Node.js, or Go toolchain
// degrades to a warning rather than failing the whole command, unless
// RequireVerify is set.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type verifyOptions struct {
	Root          string
	OpenBrowser   bool
	Timeout       time.Duration
	RequireVerify bool
	Stdout        io.Writer
}

func verifyApplication(opts verifyOptions) error {
	out := opts.Stdout
	if out == nil {
		out = os.Stdout
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 120 * time.Second
	}
	fmt.Fprintln(out, "\n==> Verifying the generated application")

	ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
	defer cancel()

	workDir, err := os.MkdirTemp("", "fh-init-verify-*")
	if err != nil {
		return degrade(opts, out, fmt.Errorf("create a working directory: %w", err))
	}
	defer os.RemoveAll(workDir)

	if err := buildFrontend(ctx, opts.Root, out); err != nil {
		return degrade(opts, out, err)
	}

	binary := filepath.Join(workDir, "server")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	if err := buildServerBinary(ctx, opts.Root, binary, out); err != nil {
		return degrade(opts, out, err)
	}

	addr, err := freeLoopbackAddr()
	if err != nil {
		return degrade(opts, out, fmt.Errorf("allocate a loopback port: %w", err))
	}
	username, password := readDevCredentials(opts.Root)

	proc, err := startServer(opts.Root, binary, addr)
	if err != nil {
		return degrade(opts, out, fmt.Errorf("start the server: %w", err))
	}
	defer stopServer(proc)

	baseURL := "http://" + addr
	if err := waitHealthy(ctx, baseURL); err != nil {
		fmt.Fprintln(out, tail(proc.output.String(), 40))
		return degrade(opts, out, fmt.Errorf("server did not become healthy: %w", err))
	}

	if err := runChecklist(ctx, baseURL, username, password, out); err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}
	fmt.Fprintln(out, "✓ All checks passed.")

	if opts.OpenBrowser {
		if err := openBrowser(baseURL); err != nil {
			fmt.Fprintf(out, "  (could not open a browser automatically: %v; open %s yourself)\n", err, baseURL)
		} else {
			fmt.Fprintf(out, "Opened %s in your browser for a live look (dev login: %s / %s).\n", baseURL, username, password)
			time.Sleep(2500 * time.Millisecond)
		}
	}
	return nil
}

// degrade turns an environment-setup failure (no network, no Go toolchain,
// no free port, ...) into a warning: the scaffold itself already succeeded,
// and -verify is a best-effort convenience on top of it, unless the caller
// explicitly asked to require it.
func degrade(opts verifyOptions, out io.Writer, err error) error {
	if opts.RequireVerify {
		return err
	}
	fmt.Fprintf(out, "  ! skipping live verification: %v\n", err)
	fmt.Fprintln(out, "  The generated project is otherwise ready. Run `go mod tidy && ./run.sh` and open it yourself.")
	return nil
}

// buildFrontend produces web/public/app.js and app.css - build output that
// is never checked into the template (see .gitignore.tmpl) - so -verify can
// exercise the real HTTP surface a browser depends on instead of a 404 at
// /assets/*. Checking for npm explicitly, rather than letting exec.Command
// fail, gives degrade() a clear "Node.js not found" message instead of an
// opaque exec error.
func buildFrontend(ctx context.Context, root string, out io.Writer) error {
	if _, err := exec.LookPath("npm"); err != nil {
		return fmt.Errorf("npm was not found on PATH (Node.js 22.6+ is required to build the frontend): %w", err)
	}
	fmt.Fprintln(out, "  building the frontend (npm install, lithe build)...")
	frontendDir := filepath.Join(root, "web", "frontend")
	if err := runNpm(ctx, frontendDir, "install"); err != nil {
		return fmt.Errorf("npm install: %w", err)
	}
	if err := runNpm(ctx, frontendDir, "run", "build"); err != nil {
		return fmt.Errorf("npm run build: %w", err)
	}
	return nil
}

func runNpm(ctx context.Context, dir string, args ...string) error {
	cmd := exec.CommandContext(ctx, "npm", args...)
	cmd.Dir = dir
	var combined bytes.Buffer
	cmd.Stdout = &combined
	cmd.Stderr = &combined
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w\n%s", err, tail(combined.String(), 40))
	}
	return nil
}

func buildServerBinary(ctx context.Context, root, binary string, out io.Writer) error {
	fmt.Fprintln(out, "  building the server (go mod tidy, go build)...")
	if err := runGo(ctx, root, "mod", "tidy"); err != nil {
		return fmt.Errorf("go mod tidy: %w", err)
	}
	if err := runGo(ctx, root, "build", "-o", binary, "./cmd/server"); err != nil {
		return fmt.Errorf("go build: %w", err)
	}
	return nil
}

func runGo(ctx context.Context, dir string, args ...string) error {
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = dir
	var combined bytes.Buffer
	cmd.Stdout = &combined
	cmd.Stderr = &combined
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w\n%s", err, tail(combined.String(), 40))
	}
	return nil
}

func tail(s string, lines int) string {
	parts := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(parts) > lines {
		parts = parts[len(parts)-lines:]
	}
	return strings.Join(parts, "\n")
}

func freeLoopbackAddr() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return "", err
	}
	return addr, nil
}

// readDevCredentials reads APP_LOGIN_USER/APP_LOGIN_PASSWORD out of the
// generated .env, falling back to the documented development defaults if
// either is absent (for example because a prior run's .env was preserved
// with different values by -force).
func readDevCredentials(root string) (username, password string) {
	username, password = "admin", "change-me-in-development"
	file, err := os.Open(filepath.Join(root, ".env"))
	if err != nil {
		return username, password
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		value = strings.Trim(strings.TrimSpace(value), `'"`)
		switch strings.TrimSpace(key) {
		case "APP_LOGIN_USER":
			if value != "" {
				username = value
			}
		case "APP_LOGIN_PASSWORD":
			if value != "" {
				password = value
			}
		}
	}
	// A scan error here just means falling back to whatever was already
	// found (or the documented defaults); this is a best-effort read for
	// -verify, not the authoritative config loader.
	_ = scanner.Err()
	return username, password
}

type serverProcess struct {
	cmd    *exec.Cmd
	output *bytes.Buffer
}

func startServer(root, binary, addr string) (*serverProcess, error) {
	cmd := exec.Command(binary)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "APP_ADDR="+addr, "APP_ORIGIN="+addr)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &serverProcess{cmd: cmd, output: &output}, nil
}

func stopServer(proc *serverProcess) {
	if proc == nil || proc.cmd.Process == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		_ = proc.cmd.Wait()
		close(done)
	}()
	_ = proc.cmd.Process.Signal(os.Interrupt)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = proc.cmd.Process.Kill()
		<-done
	}
}

func waitHealthy(ctx context.Context, baseURL string) error {
	client := &http.Client{Timeout: 3 * time.Second}
	deadline := time.After(0)
	var lastErr error
	for {
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("%w (last error: %v)", ctx.Err(), lastErr)
			}
			return ctx.Err()
		case <-deadline:
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/healthz", nil)
		if err == nil {
			resp, doErr := client.Do(req)
			if doErr == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					return nil
				}
				lastErr = fmt.Errorf("GET /healthz returned %d", resp.StatusCode)
			} else {
				lastErr = doErr
			}
		} else {
			lastErr = err
		}
		deadline = time.After(150 * time.Millisecond)
	}
}

func openBrowser(url string) error {
	name, args := openBrowserCommand(runtime.GOOS, url)
	if name == "" {
		return fmt.Errorf("no known way to open a browser on %s", runtime.GOOS)
	}
	return exec.Command(name, args...).Start()
}

func openBrowserCommand(goos, url string) (string, []string) {
	switch goos {
	case "darwin":
		return "open", []string{url}
	case "windows":
		return "rundll32", []string{"url.dll,FileProtocolHandler", url}
	default:
		return "xdg-open", []string{url}
	}
}

// --- HTTP checklist ---------------------------------------------------

type step struct {
	name string
	run  func(ctx context.Context, client *http.Client, base string) error
}

// runChecklist exercises the exact surface a signed-in browser needs,
// mirroring the "Basic verification" and "Expected security checks"
// sections of the generated README: static assets and the WASM transport
// bundle load, /api/* refuses a plain request, and the full
// login/secure-bootstrap/session/logout lifecycle over same-origin cookies
// behaves as documented.
func runChecklist(ctx context.Context, base, username, password string, out io.Writer) error {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return err
	}
	client := &http.Client{Jar: jar, Timeout: 10 * time.Second}

	steps := []step{
		{"GET / serves the application shell", checkIndexPage},
		{"GET /assets/app.js", checkAsset("/assets/app.js")},
		{"GET /assets/app.css", checkAsset("/assets/app.css")},
		{"GET /wasm/securefetch.wasm", checkAsset("/wasm/securefetch.wasm")},
		{"GET /wasm/wasm_exec.js", checkAsset("/wasm/wasm_exec.js")},
		{"GET /wasm/index.js", checkAsset("/wasm/index.js")},
		{"GET /wasm/secure-fetch.js", checkAsset("/wasm/secure-fetch.js")},
		{"GET /healthz", checkStatus(http.MethodGet, "/healthz", http.StatusOK)},
		{"GET /auth/session (signed out)", checkSessionState(false)},
		{"GET /api/me without secure transport is refused", checkStatus(http.MethodGet, "/api/me", http.StatusUpgradeRequired)},
		{"POST /auth/login", checkLogin(username, password)},
		{"GET /auth/session (signed in)", checkSessionState(true)},
		{"POST /secure-config.json", checkSecureConfig},
		{"POST /auth/logout", checkStatusOrigin(http.MethodPost, "/auth/logout", http.StatusOK, true)},
		{"GET /auth/session (signed out again)", checkSessionState(false)},
	}

	for _, s := range steps {
		if err := s.run(ctx, client, base); err != nil {
			fmt.Fprintf(out, "  ✗ %s: %v\n", s.name, err)
			return fmt.Errorf("%s: %w", s.name, err)
		}
		fmt.Fprintf(out, "  ✓ %s\n", s.name)
	}
	return nil
}

func checkIndexPage(ctx context.Context, client *http.Client, base string) error {
	resp, body, err := doRequest(ctx, client, http.MethodGet, base+"/", nil, "")
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("expected 200, got %d", resp.StatusCode)
	}
	text := string(body)
	if !strings.Contains(text, `id="app"`) {
		return fmt.Errorf("page is missing the #app mount point")
	}
	if !strings.Contains(text, `/assets/app.js`) {
		return fmt.Errorf("page is missing the boot script tag")
	}
	if strings.Count(strings.ToLower(text), "<script") != 1 {
		return fmt.Errorf("expected exactly one <script> tag in the secure SPL page")
	}
	return nil
}

func checkAsset(path string) func(ctx context.Context, client *http.Client, base string) error {
	return func(ctx context.Context, client *http.Client, base string) error {
		resp, body, err := doRequest(ctx, client, http.MethodGet, base+path, nil, "")
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("expected 200, got %d", resp.StatusCode)
		}
		if len(body) == 0 {
			return fmt.Errorf("response body is empty")
		}
		return nil
	}
}

func checkStatus(method, path string, want int) func(ctx context.Context, client *http.Client, base string) error {
	return checkStatusOrigin(method, path, want, false)
}

// checkStatusOrigin is checkStatus for endpoints that enforce the server's
// same-origin policy (see routes.go's sameOrigin): those need a matching
// Origin header or they fail closed with 403 regardless of the session
// cookie, which is correct server behavior, not something to work around.
func checkStatusOrigin(method, path string, want int, sameOrigin bool) func(ctx context.Context, client *http.Client, base string) error {
	return func(ctx context.Context, client *http.Client, base string) error {
		origin := ""
		if sameOrigin {
			origin = base
		}
		resp, _, err := doRequest(ctx, client, method, base+path, nil, origin)
		if err != nil {
			return err
		}
		if resp.StatusCode != want {
			return fmt.Errorf("expected %d, got %d", want, resp.StatusCode)
		}
		return nil
	}
}

func checkSessionState(authenticated bool) func(ctx context.Context, client *http.Client, base string) error {
	return func(ctx context.Context, client *http.Client, base string) error {
		resp, body, err := doRequest(ctx, client, http.MethodGet, base+"/auth/session", nil, "")
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("expected 200, got %d", resp.StatusCode)
		}
		var status struct {
			Authenticated bool `json:"authenticated"`
		}
		if err := json.Unmarshal(body, &status); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
		if status.Authenticated != authenticated {
			return fmt.Errorf("expected authenticated=%v, got %v", authenticated, status.Authenticated)
		}
		return nil
	}
}

func checkLogin(username, password string) func(ctx context.Context, client *http.Client, base string) error {
	return func(ctx context.Context, client *http.Client, base string) error {
		payload, err := json.Marshal(map[string]string{"username": username, "password": password})
		if err != nil {
			return err
		}
		resp, _, err := doRequest(ctx, client, http.MethodPost, base+"/auth/login", payload, base)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("expected 200, got %d (check APP_LOGIN_USER/APP_LOGIN_PASSWORD in .env)", resp.StatusCode)
		}
		return nil
	}
}

func checkSecureConfig(ctx context.Context, client *http.Client, base string) error {
	resp, body, err := doRequest(ctx, client, http.MethodPost, base+"/secure-config.json", nil, base)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("expected 200, got %d", resp.StatusCode)
	}
	var config struct {
		WasmURL           string `json:"wasmURL"`
		RegistrationToken string `json:"registrationToken"`
		PinnedServerKey   string `json:"pinnedServerKey"`
	}
	if err := json.Unmarshal(body, &config); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if config.WasmURL == "" || config.RegistrationToken == "" || config.PinnedServerKey == "" {
		return fmt.Errorf("bootstrap response is missing required fields")
	}
	// Establishing the encrypted session itself (WebCrypto + IndexedDB +
	// the WASM runtime) is browser-only; that is exactly what opening a
	// real browser at the end of -verify covers.
	return nil
}

func doRequest(ctx context.Context, client *http.Client, method, url string, body []byte, origin string) (*http.Response, []byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return nil, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if origin != "" {
		req.Header.Set("Origin", origin)
		req.Header.Set("Sec-Fetch-Site", "same-origin")
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	return resp, data, nil
}
