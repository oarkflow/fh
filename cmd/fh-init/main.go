package main

import (
	"embed"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

//go:embed template/**
var templateFS embed.FS

var modulePattern = regexp.MustCompile(`^[A-Za-z0-9._/-]+$`)

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

	return fs.WalkDir(templateFS, "template", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		rel := strings.TrimPrefix(path, "template/")
		rel = strings.TrimSuffix(rel, ".tmpl")
		destination := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
			return err
		}
		data, err := fs.ReadFile(templateFS, path)
		if err != nil {
			return err
		}
		data = []byte(strings.ReplaceAll(string(data), "__FH_MODULE__", *module))
		mode := os.FileMode(0o644)
		if filepath.Base(destination) == "run.sh" {
			mode = 0o755
		}
		return os.WriteFile(destination, data, mode)
	})
}
