// Command fh-init creates a new application from the secure fh starter.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
)

const generatorVersion = "0.1.0"

var modulePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._~/-]*(?:\.[A-Za-z]{2,})/[A-Za-z0-9._~/-]+$`)

func main() {
	module := flag.String("module", "", "Go module path (required)")
	dir := flag.String("dir", ".", "destination directory")
	name := flag.String("app-name", "", "application name (defaults to the final module path segment)")
	force := flag.Bool("force", false, "allow writing into a non-empty destination")
	dryRun := flag.Bool("dry-run", false, "list files without writing them")
	localReplace := flag.Bool("local-replace", false, "use this repository's fh checkout for template development")
	version := flag.Bool("version", false, "print generator version")
	flag.Parse()
	if *version {
		fmt.Println(generatorVersion)
		return
	}
	if err := run(*module, *dir, *name, *force, *dryRun, *localReplace); err != nil {
		fmt.Fprintln(os.Stderr, "fh-init:", err)
		os.Exit(1)
	}
}

func run(module, destination, appName string, force, dryRun, localReplace bool) error {
	if !modulePattern.MatchString(module) {
		return errors.New("-module must be a valid module path such as example.com/acme/orders")
	}
	if appName == "" {
		appName = filepath.Base(strings.TrimSuffix(module, "/"))
	}
	if !regexp.MustCompile(`^[a-z][a-z0-9-]*$`).MatchString(appName) {
		return errors.New("-app-name must contain lowercase letters, digits, and hyphens")
	}
	root, err := templateRoot()
	if err != nil {
		return err
	}
	files, err := templateFiles(root)
	if err != nil {
		return err
	}
	frameworkRoot := filepath.Clean(filepath.Join(root, "..", ".."))
	configPrefix := strings.ToUpper(strings.ReplaceAll(appName, "-", "_")) + "_"
	destination, err = filepath.Abs(destination)
	if err != nil {
		return err
	}
	if info, statErr := os.Stat(destination); statErr == nil && info.IsDir() && !force {
		entries, readErr := os.ReadDir(destination)
		if readErr != nil {
			return readErr
		}
		if len(entries) > 0 {
			return fmt.Errorf("destination %q is not empty; use -force to allow overwrite", destination)
		}
	} else if statErr != nil && !os.IsNotExist(statErr) {
		return statErr
	}
	for _, name := range files {
		out := filepath.Join(destination, name)
		if dryRun {
			fmt.Println(out)
			continue
		}
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			return err
		}
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			return err
		}
		text := strings.ReplaceAll(string(data), "github.com/oarkflow/fh/examples/template", module)
		text = strings.ReplaceAll(text, "{{APP_NAME}}", appName)
		text = strings.ReplaceAll(text, "{{MODULE_PATH}}", module)
		text = strings.ReplaceAll(text, "APP_", configPrefix)
		if name == "go.mod" && localReplace {
			text = strings.ReplaceAll(text, "replace github.com/oarkflow/fh => ../..", "replace github.com/oarkflow/fh => "+filepath.ToSlash(frameworkRoot))
		} else if name == "go.mod" {
			text = strings.ReplaceAll(text, "\nreplace github.com/oarkflow/fh => ../..", "")
		}
		if err := os.WriteFile(out, []byte(text), 0o644); err != nil {
			return err
		}
	}
	if !dryRun {
		fmt.Printf("created %s in %s\n", appName, destination)
	}
	return nil
}

func templateRoot() (string, error) {
	if cwd, err := os.Getwd(); err == nil {
		for current := cwd; ; current = filepath.Dir(current) {
			candidate := filepath.Join(current, "examples", "template")
			if isDir(candidate) {
				return candidate, nil
			}
			parent := filepath.Dir(current)
			if parent == current {
				break
			}
		}
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "examples", "template"))
	if isDir(root) {
		return root, nil
	}
	return "", errors.New("canonical template not found; run fh-init from the fh repository")
}

func templateFiles(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files = append(files, rel)
		return nil
	})
	sort.Strings(files)
	return files, err
}

func isDir(path string) bool { info, err := os.Stat(path); return err == nil && info.IsDir() }
