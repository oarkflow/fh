package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"go/format"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// modulesDir is the template directory holding opt-in features. Each module is
// modules/<name>/module.json plus modules/<name>/files/, a tree that mirrors the
// generated project and is rendered over the base scaffold.
const modulesDir = "modules"

var moduleNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// moduleSpec is modules/<name>/module.json.
type moduleSpec struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Requires    []string `json:"requires"`
	// Package is the module's import path relative to the generated project.
	// It must export Module() modules.Module (see internal/modules).
	Package string `json:"package"`
	// Env lines are appended to .env.example.
	Env []string `json:"env"`
	// GoMod lists extra Go dependencies, appended to go.mod as requirements.
	GoMod struct {
		Require map[string]string `json:"require"`
	} `json:"gomod"`
}

func splitList(value string) []string {
	var out []string
	for _, part := range strings.Split(value, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func readModuleSpec(source, name string) (moduleSpec, error) {
	var spec moduleSpec
	if !moduleNamePattern.MatchString(name) {
		return spec, fmt.Errorf("invalid module name %q", name)
	}
	raw, err := os.ReadFile(filepath.Join(source, modulesDir, name, "module.json"))
	if errors.Is(err, os.ErrNotExist) {
		return spec, fmt.Errorf("unknown module %q (see -list-modules)", name)
	}
	if err != nil {
		return spec, err
	}
	if err := json.Unmarshal(raw, &spec); err != nil {
		return spec, fmt.Errorf("module %s: %w", name, err)
	}
	if spec.Name != name || spec.Package == "" {
		return spec, fmt.Errorf("module %s: module.json needs a matching name and a package", name)
	}
	return spec, nil
}

// resolveModules returns the requested modules plus everything they require,
// ordered so that dependencies come first.
func resolveModules(source string, requested []string) ([]moduleSpec, error) {
	var ordered []moduleSpec
	state := map[string]int{} // 1 visiting, 2 done
	var visit func(name string, chain []string) error
	visit = func(name string, chain []string) error {
		switch state[name] {
		case 2:
			return nil
		case 1:
			return fmt.Errorf("module dependency cycle: %s", strings.Join(append(chain, name), " -> "))
		}
		state[name] = 1
		spec, err := readModuleSpec(source, name)
		if err != nil {
			return err
		}
		for _, dep := range spec.Requires {
			if err := visit(dep, append(chain, name)); err != nil {
				return err
			}
		}
		state[name] = 2
		ordered = append(ordered, spec)
		return nil
	}
	for _, name := range requested {
		if err := visit(name, nil); err != nil {
			return nil, err
		}
	}
	return ordered, nil
}

// applyModules renders each module's files over the scaffold, appends its
// environment defaults and regenerates internal/modules/enabled.
func applyModules(source, root, module string, mods []moduleSpec) error {
	if len(mods) == 0 {
		return nil
	}
	var env strings.Builder
	for _, spec := range mods {
		files := filepath.Join(source, modulesDir, spec.Name, "files")
		err := filepath.WalkDir(files, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil || entry.IsDir() {
				return walkErr
			}
			rel, err := filepath.Rel(files, path)
			if err != nil {
				return err
			}
			return renderFile(path, filepath.Join(root, strings.TrimSuffix(rel, ".tmpl")), module)
		})
		if err != nil {
			return fmt.Errorf("module %s: %w", spec.Name, err)
		}
		if len(spec.Env) > 0 {
			env.WriteString("\n# --- module: ");env.WriteString(spec.Name);env.WriteString(" ---\n");env.WriteString(strings.Join(spec.Env, "\n"));env.WriteString("\n")
		}
	}
	requires := map[string]string{}
	for _, spec := range mods {
		for path, version := range spec.GoMod.Require {
			requires[path] = version
		}
	}
	if len(requires) > 0 {
		paths := make([]string, 0, len(requires))
		for path := range requires {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		var b strings.Builder
		b.WriteString("\nrequire (\n")
		for _, path := range paths {
			b.WriteString("\t");b.WriteString(path);b.WriteString(" ");b.WriteString(requires[path]);b.WriteString("\n")
		}
		b.WriteString(")\n")
		gm, err := os.OpenFile(filepath.Join(root, "go.mod"), os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		_, werr := gm.WriteString(b.String())
		if cerr := gm.Close(); werr == nil {
			werr = cerr
		}
		if werr != nil {
			return werr
		}
	}
	if env.Len() > 0 {
		f, err := os.OpenFile(filepath.Join(root, ".env.example"), os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		_, werr := f.WriteString(env.String())
		if cerr := f.Close(); werr == nil {
			werr = cerr
		}
		if werr != nil {
			return werr
		}
	}
	formatted, err := format.Source([]byte(renderEnabled(module, mods)))
	if err != nil {
		return fmt.Errorf("generate enabled modules: %w", err)
	}
	return os.WriteFile(filepath.Join(root, "internal", "modules", "enabled", "enabled.go"), formatted, 0o644)
}

func renderEnabled(module string, mods []moduleSpec) string {
	var b strings.Builder
	b.WriteString("// Code generated by fh-init; DO NOT EDIT.\n\n")
	b.WriteString("// Package enabled lists the modules compiled into this application, in\n// dependency order.\npackage enabled\n\nimport (\n")
	b.WriteString(fmt.Sprintf("\t%q\n", module+"/internal/modules"))
	for _, spec := range mods {
		b.WriteString(fmt.Sprintf("\t%s %q\n", importAlias(spec.Name), module+"/"+spec.Package))
	}
	b.WriteString(")\n\nfunc All() []modules.Module {\n\treturn []modules.Module{\n")
	for _, spec := range mods {
		b.WriteString(fmt.Sprintf("\t\t%s.Module(),\n", importAlias(spec.Name)))
	}
	b.WriteString("\t}\n}\n")
	return b.String()
}

func importAlias(name string) string {
	return strings.ReplaceAll(name, "-", "_") + "mod"
}

// printModules lists the opt-in modules of the configured template.
func printModules(w io.Writer) error {
	workDir, err := os.MkdirTemp("", "fh-init-modules-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(workDir)
	source := filepath.Join(workDir, "template")
	if err := cloneRepository(templateRepo, source, "template"); err != nil {
		return err
	}
	entries, err := os.ReadDir(filepath.Join(source, modulesDir))
	if err != nil {
		return errors.New("this template offers no opt-in modules")
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		spec, err := readModuleSpec(source, name)
		if err != nil {
			return err
		}
		requires := ""
		if len(spec.Requires) > 0 {
			requires = " (requires " + strings.Join(spec.Requires, ", ") + ")"
		}
		fmt.Fprintf(w, "%-12s %s%s\n", spec.Name, spec.Description, requires)
	}
	return nil
}

// frontendModulesDir is where the frontend repository keeps the views of its
// opt-in modules, next to src/. Each is modules/<name>/module.json plus a src/ tree
// mirroring the application's src/.
const frontendModulesDir = "modules"

type frontendModule struct {
	Name  string `json:"name"`
	Index string `json:"index"` // path of the file exporting the module's ModuleDef[] as default
}

// applyFrontendModules copies the views of every enabled module that has any into
// web/frontend/src and lists them in src/modules/registry.ts. The modules directory
// itself is removed afterwards: a generated project carries only what it uses.
func applyFrontendModules(root string, mods []moduleSpec) error {
	frontend := filepath.Join(root, "web", "frontend")
	available := filepath.Join(frontend, frontendModulesDir)
	defer os.RemoveAll(available)
	var found []frontendModule
	for _, spec := range mods {
		dir := filepath.Join(available, spec.Name)
		raw, err := os.ReadFile(filepath.Join(dir, "module.json"))
		if errors.Is(err, os.ErrNotExist) {
			continue // a backend-only module
		}
		if err != nil {
			return err
		}
		var fm frontendModule
		if err := json.Unmarshal(raw, &fm); err != nil || fm.Index == "" {
			return fmt.Errorf("frontend module %s: module.json needs an index", spec.Name)
		}
		if err := copyTree(filepath.Join(dir, "src"), filepath.Join(frontend, "src")); err != nil {
			return fmt.Errorf("frontend module %s: %w", spec.Name, err)
		}
		found = append(found, fm)
	}
	if len(found) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString("// Generated by fh-init; lists the modules compiled into this application.\n")
	b.WriteString("import type { ModuleDef } from './types.ts';\n")
	for _, fm := range found {
		// index paths are relative to the frontend root; the registry lives in src/modules/.
		b.WriteString(fmt.Sprintf("import %s from './%s';\n", importAlias(fm.Name), strings.TrimPrefix(fm.Index, "src/modules/")))
	}
	b.WriteString("\nexport const modules: ModuleDef[] = [\n")
	for _, fm := range found {
		b.WriteString(fmt.Sprintf("  ...%s,\n", importAlias(fm.Name)))
	}
	b.WriteString("];\n")
	return os.WriteFile(filepath.Join(frontend, "src", "modules", "registry.ts"), []byte(b.String()), 0o644)
}
