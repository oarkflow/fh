package dagflow

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

type BCLFileInfo struct {
	Path    string    `json:"path"`
	Name    string    `json:"name"`
	Dir     string    `json:"dir"`
	Size    int64     `json:"size"`
	ModTime time.Time `json:"mod_time"`
}

type BCLFileContent struct {
	Path    string    `json:"path"`
	Content string    `json:"content"`
	Size    int64     `json:"size"`
	ModTime time.Time `json:"mod_time"`
}

type BCLSaveRequest struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

type BCLValidateRequest struct {
	Path    string `json:"path,omitempty"`
	Content string `json:"content,omitempty"`
}

type BCLValidateResponse struct {
	Valid      bool     `json:"valid"`
	Error      string   `json:"error,omitempty"`
	Workflows  int      `json:"workflows"`
	Routes     int      `json:"routes"`
	Schemas    int      `json:"schemas"`
	Chains     int      `json:"chains"`
	Scripts    int      `json:"scripts"`
	Conditions int      `json:"conditions"`
	Files      []string `json:"files,omitempty"`
}

func RegisterBCLAdmin(app *fh.App, engine *Engine, cfg *Config, bclRoot string) {
	root := bclRoot
	if root == "" {
		root = DefaultBCLPath()
	}
	app.Get("/ops/bcl/files", opsGuard(bclFiles(root)))
	app.Get("/ops/bcl/file", opsGuard(bclGetFile(root)))
	app.Post("/ops/bcl/file", opsGuard(bclSaveFile(root)))
	app.Put("/ops/bcl/file", opsGuard(bclSaveFile(root)))
	app.Delete("/ops/bcl/file", opsGuard(bclDeleteFile(root)))
	app.Post("/ops/bcl/validate", opsGuard(bclValidate(root, engine)))
	app.Get("/ops/bcl/config", opsGuard(bclConfig(root)))
	app.Get("/ops/bcl/templates", opsGuard(bclTemplates()))
	_ = cfg
}

func bclFiles(root string) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		files, err := listBCLFiles(root)
		if err != nil {
			return writeJSON(c, fh.StatusInternalServerError, map[string]any{"error": err.Error()})
		}
		return writeJSON(c, fh.StatusOK, files)
	}
}

func bclGetFile(root string) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		path := c.Query("path")
		abs, rel, err := safeBCLPath(root, path)
		if err != nil {
			return writeJSON(c, fh.StatusBadRequest, map[string]any{"error": err.Error()})
		}
		st, err := os.Stat(abs)
		if err != nil {
			return writeJSON(c, fh.StatusNotFound, map[string]any{"error": err.Error()})
		}
		if st.IsDir() {
			return writeJSON(c, fh.StatusBadRequest, map[string]any{"error": "path is a directory"})
		}
		b, err := os.ReadFile(abs)
		if err != nil {
			return writeJSON(c, fh.StatusInternalServerError, map[string]any{"error": err.Error()})
		}
		return writeJSON(c, fh.StatusOK, BCLFileContent{Path: rel, Content: string(b), Size: st.Size(), ModTime: st.ModTime()})
	}
}

func bclSaveFile(root string) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		var req BCLSaveRequest
		if err := json.Unmarshal(c.Body(), &req); err != nil {
			return writeJSON(c, fh.StatusBadRequest, map[string]any{"error": "invalid JSON body", "detail": err.Error()})
		}
		abs, rel, err := safeBCLPath(root, req.Path)
		if err != nil {
			return writeJSON(c, fh.StatusBadRequest, map[string]any{"error": err.Error()})
		}
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			return writeJSON(c, fh.StatusInternalServerError, map[string]any{"error": err.Error()})
		}
		if err := atomicWriteFile(abs, []byte(req.Content), 0o644); err != nil {
			return writeJSON(c, fh.StatusInternalServerError, map[string]any{"error": err.Error()})
		}
		st, _ := os.Stat(abs)
		return writeJSON(c, fh.StatusOK, map[string]any{"saved": true, "path": rel, "size": st.Size(), "mod_time": st.ModTime(), "restart_required": true})
	}
}

func bclDeleteFile(root string) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		abs, rel, err := safeBCLPath(root, c.Query("path"))
		if err != nil {
			return writeJSON(c, fh.StatusBadRequest, map[string]any{"error": err.Error()})
		}
		if err := os.Remove(abs); err != nil {
			return writeJSON(c, fh.StatusNotFound, map[string]any{"error": err.Error()})
		}
		return writeJSON(c, fh.StatusOK, map[string]any{"deleted": true, "path": rel, "restart_required": true})
	}
}

func bclValidate(root string, engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		var req BCLValidateRequest
		_ = json.Unmarshal(c.Body(), &req)
		validateRoot := root
		cleanup := func() {}
		if req.Path != "" || req.Content != "" {
			tmp, err := copyBCLDir(root)
			if err != nil {
				return writeJSON(c, fh.StatusInternalServerError, map[string]any{"valid": false, "error": err.Error()})
			}
			cleanup = func() { _ = os.RemoveAll(tmp) }
			validateRoot = tmp
			if req.Path != "" {
				abs, _, err := safeBCLPath(validateRoot, req.Path)
				if err != nil {
					cleanup()
					return writeJSON(c, fh.StatusBadRequest, map[string]any{"valid": false, "error": err.Error()})
				}
				if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
					cleanup()
					return writeJSON(c, fh.StatusInternalServerError, map[string]any{"valid": false, "error": err.Error()})
				}
				if err := os.WriteFile(abs, []byte(req.Content), 0o644); err != nil {
					cleanup()
					return writeJSON(c, fh.StatusInternalServerError, map[string]any{"valid": false, "error": err.Error()})
				}
			}
		}
		defer cleanup()
		resp, status := validateBCLRoot(validateRoot, engine)
		return writeJSON(c, status, resp)
	}
}

func bclConfig(root string) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		cfg, err := LoadBCL(root)
		if err != nil {
			return writeJSON(c, fh.StatusBadRequest, map[string]any{"error": err.Error()})
		}
		return writeJSON(c, fh.StatusOK, cfg)
	}
}

func bclTemplates() fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		return writeJSON(c, fh.StatusOK, map[string]string{
			"workflow": `workflow "new_workflow" {
  name "New Workflow"
  version "1.0.0"
  first "start"

  node "start" {
    type function
    handler "validate_email"
  }
}
`,
			"route": `route "new_route" {
  method "POST"
  path "/api/new"
  workflow "new_workflow"
  mode sync
}
`,
			"schema": `schema "NewRequest" {
  type object
  field "name" {
    type string
    required true
  }
}
`,
			"script": `script "new_script" {
  source "return input"
}
`,
		})
	}
}

func validateBCLRoot(root string, engine *Engine) (BCLValidateResponse, int) {
	cfg, err := LoadBCL(root)
	if err != nil {
		return BCLValidateResponse{Valid: false, Error: err.Error()}, fh.StatusBadRequest
	}
	candidate := NewEngine(NewMemoryTaskStore(), NewMemoryChainStore(), NewMemoryBroker())
	copyEngineRegistries(engine, candidate)
	if err := candidate.LoadConfig(cfg); err != nil {
		return BCLValidateResponse{Valid: false, Error: err.Error()}, fh.StatusBadRequest
	}
	if err := ValidateConfig(cfg, candidate); err != nil {
		return BCLValidateResponse{Valid: false, Error: err.Error()}, fh.StatusBadRequest
	}
	files, _ := listBCLFiles(root)
	fileNames := make([]string, 0, len(files))
	for _, f := range files {
		fileNames = append(fileNames, f.Path)
	}
	return BCLValidateResponse{Valid: true, Workflows: len(cfg.Workflows), Routes: len(FlattenRoutes(cfg)), Schemas: len(cfg.Schemas), Chains: len(cfg.Chains), Scripts: len(cfg.Scripts), Conditions: len(cfg.Conditions), Files: fileNames}, fh.StatusOK
}

func copyEngineRegistries(src, dst *Engine) {
	if src == nil || dst == nil {
		return
	}
	src.mu.RLock()
	defer src.mu.RUnlock()
	for k, v := range src.handlers {
		dst.handlers[k] = v
	}
	for k, v := range src.dataSources {
		dst.dataSources[k] = v
	}
}

func listBCLFiles(root string) ([]BCLFileInfo, error) {
	base, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	var out []BCLFileInfo
	err = filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if strings.HasPrefix(name, ".") || name == "node_modules" || name == "dist" {
				if path != base {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if strings.ToLower(filepath.Ext(path)) != ".bcl" {
			return nil
		}
		st, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(base, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		out = append(out, BCLFileInfo{Path: rel, Name: filepath.Base(rel), Dir: filepath.ToSlash(filepath.Dir(rel)), Size: st.Size(), ModTime: st.ModTime()})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

func safeBCLPath(root, rel string) (string, string, error) {
	if strings.TrimSpace(rel) == "" {
		return "", "", fmt.Errorf("path is required")
	}
	rel = filepath.ToSlash(strings.TrimSpace(rel))
	rel = strings.TrimPrefix(rel, "/")
	clean := filepath.Clean(filepath.FromSlash(rel))
	if clean == "." || strings.HasPrefix(clean, "..") || filepath.IsAbs(clean) {
		return "", "", fmt.Errorf("invalid BCL path")
	}
	if strings.ToLower(filepath.Ext(clean)) != ".bcl" {
		return "", "", fmt.Errorf("only .bcl files are allowed")
	}
	base, err := filepath.Abs(root)
	if err != nil {
		return "", "", err
	}
	abs, err := filepath.Abs(filepath.Join(base, clean))
	if err != nil {
		return "", "", err
	}
	if abs != base && !strings.HasPrefix(abs, base+string(os.PathSeparator)) {
		return "", "", fmt.Errorf("path escapes BCL root")
	}
	return abs, filepath.ToSlash(clean), nil
}

func atomicWriteFile(path string, data []byte, perm fs.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*.bcl")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, perm); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func copyBCLDir(root string) (string, error) {
	base, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	tmp, err := os.MkdirTemp("", "dagflow-bcl-validate-*")
	if err != nil {
		return "", err
	}
	err = filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(base, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		dst := filepath.Join(tmp, rel)
		if d.IsDir() {
			return os.MkdirAll(dst, 0o755)
		}
		if strings.ToLower(filepath.Ext(path)) != ".bcl" {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		return os.WriteFile(dst, b, 0o644)
	})
	if err != nil {
		_ = os.RemoveAll(tmp)
		return "", err
	}
	return tmp, nil
}
