package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var modulePattern = regexp.MustCompile(`^[A-Za-z0-9._/-]+$`)

// templateRepo is the application scaffold, cloned into a temporary directory
// and rendered into the requested output during scaffolding.
var templateRepo = "https://github.com/oarkflow/fh-template.git"

// frontendRepo is the FH Control Center frontend boilerplate, cloned into
// web/frontend during scaffolding (see cloneFrontend). It is a package
// variable, not a const, so tests can point it at a local fixture repo
// instead of performing a real network/SSH clone.
var frontendRepo = "git@oarkflow:oarkflow/lithe-boilerplate.git"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "fh-init:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fspec := flag.NewFlagSet("fh-init", flag.ContinueOnError)
	fspec.SetOutput(os.Stderr)
	module := fspec.String("module", "", "Go module path for the new application")
	output := fspec.String("dir", ".", "directory to create the application in")
	force := fspec.Bool("force", false, "allow writing into a non-empty directory")
	verify := fspec.Bool("verify", false, "after scaffolding, build the frontend and server, run the application on a loopback port, exercise its HTTP surface, and open it in your browser")
	noBrowser := fspec.Bool("no-browser", false, "with -verify, run the HTTP checklist but do not open a system browser")
	requireVerify := fspec.Bool("require-verify", false, "with -verify, fail the command if verification cannot run to completion (default: warn and still exit successfully)")
	verifyTimeout := fspec.Duration("verify-timeout", 6*time.Minute, "time budget for -verify's frontend+server build, run, and checklist phase")
	if err := fspec.Parse(args); err != nil {
		return err
	}
	if *module == "" {
		return errors.New("-module is required, for example -module example.com/acme/orders")
	}
	if !modulePattern.MatchString(*module) || strings.HasPrefix(*module, "/") || strings.Contains(*module, "..") {
		return fmt.Errorf("invalid module path %q", *module)
	}
	root, err := filepath.Abs(*output)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	if len(entries) != 0 && !*force {
		return fmt.Errorf("directory %s is not empty; use -force to continue", root)
	}

	if err := renderTemplate(root, *module); err != nil {
		return err
	}
	if err := cloneFrontend(root); err != nil {
		return err
	}
	if err := createEnvironmentFile(root); err != nil {
		return err
	}
	if !*verify {
		return nil
	}
	return verifyApplication(verifyOptions{
		Root:          root,
		OpenBrowser:   !*noBrowser,
		Timeout:       *verifyTimeout,
		RequireVerify: *requireVerify,
	})
}

func renderTemplate(root, module string) error {
	workDir, err := os.MkdirTemp("", "fh-init-template-*")
	if err != nil {
		return fmt.Errorf("create template working directory: %w", err)
	}
	defer os.RemoveAll(workDir)

	source := filepath.Join(workDir, "template")
	if err := cloneRepository(templateRepo, source, "template"); err != nil {
		return err
	}
	return filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		rel = strings.TrimSuffix(rel, ".tmpl")
		destination := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		data = []byte(strings.ReplaceAll(string(data), "__FH_MODULE__", module))
		mode := os.FileMode(0o644)
		if filepath.Base(destination) == "run.sh" {
			mode = 0o755
		}
		return os.WriteFile(destination, data, mode)
	})
}

// cloneFrontend vendors the FH Control Center frontend boilerplate
// (web/frontend/src, package.json, tsconfig.json, ...) into the generated
// project with a shallow git clone, then strips the clone's own .git
// directory so the new project owns a clean, plain copy - not a nested repo
// or something that would ever look like a submodule to the new project's
// own git init/add. The frontend is versioned and released independently of
// fh-init (see frontendRepo), rather than embedded template source, so the
// two can evolve on separate release cycles.
//
// Required, not best-effort: unlike -verify's build step, a generated
// project with no frontend at all is not a usable starting point, so a
// clone failure (missing git, no network/SSH access to frontendRepo, ...)
// fails the whole command instead of degrading to a warning.
func cloneFrontend(root string) error {
	dest := filepath.Join(root, "web", "frontend")
	fmt.Printf("Cloning frontend boilerplate from %s...\n", frontendRepo)
	return cloneRepository(frontendRepo, dest, "frontend")
}

func cloneRepository(repo, dest, label string) error {
	// -force can re-scaffold into a directory left over from a previous run.
	if err := os.RemoveAll(dest); err != nil {
		return fmt.Errorf("clear %s for a fresh %s clone: %w", dest, label, err)
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", "clone", "--depth", "1", "--quiet", repo, dest)
	var combined bytes.Buffer
	cmd.Stdout = &combined
	cmd.Stderr = &combined
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("clone %s from %s: %w\n%s", label, repo, err, strings.TrimSpace(combined.String()))
	}
	if err := os.RemoveAll(filepath.Join(dest, ".git")); err != nil {
		return fmt.Errorf("remove cloned %s git directory: %w", label, err)
	}
	return nil
}

// createEnvironmentFile gives a new application runnable development defaults.
// O_EXCL is intentional: -force may refresh generated source, but must never
// replace environment-specific values or secrets in an existing .env file.
func createEnvironmentFile(root string) error {
	data, err := os.ReadFile(filepath.Join(root, ".env.example"))
	if err != nil {
		return fmt.Errorf("read .env.example: %w", err)
	}
	destination := filepath.Join(root, ".env")
	file, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("create .env: %w", err)
	}

	_, writeErr := file.Write(data)
	closeErr := file.Close()
	if writeErr != nil {
		return fmt.Errorf("initialize .env: %w", writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close .env: %w", closeErr)
	}
	return nil
}
