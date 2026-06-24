// Package static serves files as route handlers with cache and download controls.
package static

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"mime"
	"net/http"
	"os"
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
	return func(c fh.Ctx) error {
		rel := strings.TrimPrefix(c.Param("*"), "/")
		if rel == "" {
			rel = strings.TrimPrefix(c.Path(), cfg.Prefix)
			rel = strings.TrimPrefix(rel, "/")
		}
		full := filepath.Join(root, strings.TrimPrefix(filepath.Clean("/"+rel), "/"))
		if full != root && !strings.HasPrefix(full, root+string(filepath.Separator)) {
			return c.SendStatus(fh.StatusForbidden)
		}
		info, err := os.Stat(full)
		if err != nil && cfg.SPAFallback != "" {
			full = filepath.Join(root, cfg.SPAFallback)
			info, err = os.Stat(full)
		}
		if err != nil {
			return c.SendStatus(fh.StatusNotFound)
		}
		if info.IsDir() {
			index := filepath.Join(full, cfg.Index)
			if candidate, statErr := os.Stat(index); statErr == nil {
				full, info = index, candidate
			} else if !cfg.Browse {
				return c.SendStatus(fh.StatusForbidden)
			}
		}
		if info.IsDir() {
			return c.SendStatus(fh.StatusForbidden)
		}
		body, err := os.ReadFile(full)
		if err != nil {
			return err
		}
		if contentType := mime.TypeByExtension(filepath.Ext(full)); contentType != "" {
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
			sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%d", full, info.Size(), info.ModTime().UnixNano())))
			etag := "\"" + hex.EncodeToString(sum[:8]) + "\""
			c.Set("ETag", etag)
			if c.Get("If-None-Match") == etag {
				return c.SendStatus(fh.StatusNotModified)
			}
		}
		if cfg.Download {
			c.Set("Content-Disposition", "attachment; filename=\""+filepath.Base(full)+"\"")
		}
		return c.SendBytes(body)
	}
}
