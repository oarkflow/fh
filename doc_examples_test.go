package fh_test

// This file is the driver for the documentation-example harness described in
// the project roadmap ("Repair and compile-test every documentation
// example"). It walks every docs/*.md file and README.md, extracts every
// ```go fenced code block, and checks it:
//
//   - Full standalone programs (blocks starting with `package main`/
//     `package <name>`) are compiled for real in an isolated temp module
//     that requires this repo via a `replace` directive, using `go build`.
//   - Everything else is treated as an illustrative fragment: it is checked
//     for syntax validity (go/parser, via a synthetic wrapper) and for
//     symbol existence (every `<pkg>.<Identifier>` reference to the root fh
//     package or an mw/*  or pkg/* package is checked against that
//     package's real exported top-level identifiers via `go doc`).
//
// Run it with:
//
//	go test . -run TestDocExamples -v
//
// See internal/doccheck for the implementation and its documented
// limitations.

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/oarkflow/fh/internal/doccheck"
)

const modulePath = "github.com/oarkflow/fh"

// docFiles returns every Markdown file to scan: README.md plus docs/*.md.
func docFiles(t *testing.T, repoRoot string) []string {
	t.Helper()
	var files []string
	if _, err := os.Stat(filepath.Join(repoRoot, "README.md")); err == nil {
		files = append(files, filepath.Join(repoRoot, "README.md"))
	}
	entries, err := os.ReadDir(filepath.Join(repoRoot, "docs"))
	if err != nil {
		t.Fatalf("reading docs/: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".md" {
			continue
		}
		files = append(files, filepath.Join(repoRoot, "docs", e.Name()))
	}
	return files
}

// TestDocExamples extracts and checks every ```go block in the project's
// documentation. See the package doc comment above for the two-tier
// checking strategy.
func TestDocExamples(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd: %v", err)
	}

	reg, err := doccheck.DiscoverPackages(repoRoot, modulePath)
	if err != nil {
		t.Fatalf("discovering packages: %v", err)
	}
	st := doccheck.NewSymbolTable()

	var (
		totalBlocks   int
		fullPrograms  int
		fragments     int
		filesWithCode int
	)

	for _, path := range docFiles(t, repoRoot) {
		rel, _ := filepath.Rel(repoRoot, path)
		blocks, err := doccheck.ExtractGoBlocks(path)
		if err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
		if len(blocks) == 0 {
			continue
		}
		filesWithCode++
		for _, b := range blocks {
			b.File = rel
			totalBlocks++
			name := b.Pos()

			switch doccheck.Classify(b) {
			case doccheck.KindFullProgram:
				fullPrograms++
				t.Run(name, func(t *testing.T) {
					dir := t.TempDir()
					if err := doccheck.CompileFullProgram(b, repoRoot, dir); err != nil {
						t.Errorf("full-program example failed to build:\n%v", err)
					}
				})
			case doccheck.KindFragment:
				fragments++
				t.Run(name, func(t *testing.T) {
					if err := doccheck.CheckFragmentSyntax(b); err != nil {
						t.Errorf("fragment failed syntax check: %v", err)
					}
					if problems := doccheck.CheckFragmentSymbols(b, reg, st); len(problems) > 0 {
						for _, p := range problems {
							t.Errorf("symbol check: %s", p)
						}
					}
				})
			}
		}
	}

	t.Logf("doc example harness: %d ```go blocks across %d files (%d full programs compile-tested, %d fragments syntax+symbol checked)",
		totalBlocks, filesWithCode, fullPrograms, fragments)
}
