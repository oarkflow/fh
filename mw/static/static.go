// Package static serves files as route handlers with cache and download controls.
package static

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"mime"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

type Config struct {
	Root         string
	Prefix       string
	Index        string
	Browse       bool
	ETag         bool
	LastModified bool
	CacheControl string
	MaxAge       time.Duration
	Immutable    bool
	SPAFallback  string
	Download     bool
}

func New(root string, config ...Config) fh.HandlerFunc {
	cfg := Config{Root: root, Index: "index.html"}
	if len(config) > 0 {
		cfg = config[0]
		if cfg.Root == "" {
			cfg.Root = root
		}
		if cfg.Index == "" {
			cfg.Index = "index.html"
		}
	}
	root = filepath.Clean(cfg.Root)
	rootFS, err := os.OpenRoot(root)
	if err != nil {
		panic(fmt.Errorf("static: open root %q: %w", root, err))
	}
	return func(c fh.Ctx) error {
		rel := strings.TrimPrefix(c.Param("*"), "/")
		if rel == "" {
			rel = strings.TrimPrefix(c.Path(), cfg.Prefix)
			rel = strings.TrimPrefix(rel, "/")
		}
		name := cleanRootName(rel)
		info, err := rootFS.Stat(name)
		if err != nil && cfg.SPAFallback != "" {
			name = cleanRootName(cfg.SPAFallback)
			info, err = rootFS.Stat(name)
		}
		if err != nil {
			return c.SendStatus(fh.StatusNotFound)
		}
		if info.IsDir() {
			index := path.Join(name, cfg.Index)
			if candidate, statErr := rootFS.Stat(index); statErr == nil {
				name, info = index, candidate
			} else if !cfg.Browse {
				return c.SendStatus(fh.StatusForbidden)
			}
		}
		if info.IsDir() {
			return c.SendStatus(fh.StatusForbidden)
		}
		body, err := rootFS.ReadFile(name)
		if err != nil {
			return err
		}
		if contentType := mime.TypeByExtension(path.Ext(name)); contentType != "" {
			c.Type(contentType)
		}
		if cfg.CacheControl != "" {
			c.Set("Cache-Control", cfg.CacheControl)
		} else if cfg.Immutable {
			c.Set("Cache-Control", "public, max-age=31536000, immutable")
		} else if cfg.MaxAge > 0 {
			c.Set("Cache-Control", fmt.Sprintf("public, max-age=%d", int(cfg.MaxAge.Seconds())))
		}
		if cfg.LastModified {
			c.Set("Last-Modified", info.ModTime().UTC().Format(http.TimeFormat))
		}
		if cfg.ETag {
			sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%d", name, info.Size(), info.ModTime().UnixNano())))
			etag := "\"" + hex.EncodeToString(sum[:8]) + "\""
			c.Set("ETag", etag)
			if c.Get("If-None-Match") == etag {
				return c.SendStatus(fh.StatusNotModified)
			}
		}
		if cfg.Download {
			c.Set("Content-Disposition", "attachment; filename=\""+sanitizeFilename(path.Base(name))+"\"")
		}
		return c.SendBytes(body)
	}
}

func cleanRootName(name string) string {
	name = filepath.ToSlash(strings.TrimPrefix(filepath.Clean("/"+name), string(filepath.Separator)))
	if name == "" {
		return "."
	}
	return name
}

// sanitizeFilename strips characters from a filename that could break
// Content-Disposition header quoting or enable header injection.
func sanitizeFilename(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch c {
		case '"', '\r', '\n', '\\', '/':
			b.WriteByte('_')
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}
