// Package static serves files as route handlers with cache and download controls.
package static

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"html"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/oarkflow/fh"
)

type Config struct {
	Root             string
	Prefix           string
	Index            string
	Browse           bool
	MaxBrowseEntries int
	ETag             bool
	LastModified     bool
	CacheControl     string
	MaxAge           time.Duration
	Immutable        bool
	SPAFallback      string
	Download         bool
}

const (
	defaultMaxBrowseEntries = 1000
	maxBrowseEntries        = 10000
)

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
			} else if cfg.Browse {
				return listDir(c, rootFS, name, cfg.MaxBrowseEntries)
			} else {
				return c.SendStatus(fh.StatusForbidden)
			}
		}
		if info.IsDir() {
			return c.SendStatus(fh.StatusForbidden)
		}
		file, err := rootFS.Open(name)
		if err != nil {
			return err
		}
		defer file.Close()
		info, err = file.Stat()
		if err != nil {
			return err
		}
		if info.IsDir() {
			return c.SendStatus(fh.StatusForbidden)
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
			modTime := info.ModTime().UTC()
			c.Set("Last-Modified", modTime.Format(http.TimeFormat))
		}
		etagConditionPresent := false
		etag := ""
		if cfg.ETag {
			sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%d", name, info.Size(), info.ModTime().UnixNano())))
			etag = "W/\"" + hex.EncodeToString(sum[:8]) + "\""
			c.Set("ETag", etag)
		}
		if value, present := requestHeaderValue(c, "If-None-Match"); present {
			etagConditionPresent = true
			if weakETagMatch(value, etag) {
				if c.Method() == http.MethodGet || c.Method() == http.MethodHead {
					return c.SendStatus(fh.StatusNotModified)
				}
				return c.SendStatus(fh.StatusPreconditionFailed)
			}
		}
		if cfg.LastModified && !etagConditionPresent && (c.Method() == http.MethodGet || c.Method() == http.MethodHead) {
			if value, present := requestHeaderValue(c, "If-Modified-Since"); present {
				if since, parseErr := http.ParseTime(value); parseErr == nil && !info.ModTime().UTC().Truncate(time.Second).After(since) {
					return c.SendStatus(fh.StatusNotModified)
				}
			}
		}
		if cfg.Download {
			c.Set("Content-Disposition", "attachment; filename=\""+sanitizeFilename(path.Base(name))+"\"")
		}
		return c.SendStreamLength(file, info.Size())
	}
}

func requestHeaderValue(c fh.Ctx, name string) (string, bool) {
	var value strings.Builder
	found := false
	c.RequestHeader().VisitAll(func(key, fieldValue []byte) {
		if !strings.EqualFold(string(key), name) {
			return
		}
		if found {
			value.WriteByte(',')
		}
		_, _ = value.Write(fieldValue)
		found = true
	})
	if !found {
		return "", false
	}
	return value.String(), true
}

func weakETagMatch(header, current string) bool {
	header = strings.TrimSpace(header)
	if header == "*" {
		return true
	}
	current = strings.TrimPrefix(strings.TrimSpace(current), "W/")
	matched := false
	for i := 0; i < len(header); {
		for i < len(header) && (header[i] == ' ' || header[i] == '\t' || header[i] == ',') {
			i++
		}
		if i == len(header) {
			return matched
		}
		start := i
		if strings.HasPrefix(header[i:], "W/") {
			i += 2
		}
		if i >= len(header) || header[i] != '"' {
			return false
		}
		i++
		for i < len(header) && header[i] != '"' {
			if header[i] < 0x21 || header[i] == 0x7f {
				return false
			}
			i++
		}
		if i == len(header) {
			return false
		}
		i++
		if strings.TrimPrefix(header[start:i], "W/") == current {
			matched = true
		}
		for i < len(header) && (header[i] == ' ' || header[i] == '\t') {
			i++
		}
		if i < len(header) && header[i] != ',' {
			return false
		}
	}
	return matched
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

func listDir(c fh.Ctx, rootFS *os.Root, dir string, limit int) error {
	entries, truncated, err := readDirBounded(rootFS, dir, limit)
	if err != nil {
		return err
	}

	visible := entries[:0]
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".") {
			visible = append(visible, entry)
		}
	}
	entries = visible

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].IsDir() != entries[j].IsDir() {
			return entries[i].IsDir()
		}
		return compareFolded(entries[i].Name(), entries[j].Name()) < 0
	})

	safePath := html.EscapeString(dir)

	var buf bytes.Buffer
	buf.WriteString("<!DOCTYPE html><html><head><meta charset=\"utf-8\">")
	buf.WriteString("<title>Index of ")
	buf.WriteString(safePath)
	buf.WriteString("</title></head><body>")
	buf.WriteString("<h1>Index of ")
	buf.WriteString(safePath)
	buf.WriteString("</h1><hr><ul>")

	if dir != "." {
		buf.WriteString("<li><a href=\"../\">../</a></li>")
	}

	for _, entry := range entries {
		name := html.EscapeString(entry.Name())
		href := html.EscapeString("./" + url.PathEscape(entry.Name()))
		if entry.IsDir() {
			buf.WriteString("<li><a href=\"")
			buf.WriteString(href)
			buf.WriteString("/\"><strong>")
			buf.WriteString(name)
			buf.WriteString("/</strong></a></li>")
		} else {
			buf.WriteString("<li><a href=\"")
			buf.WriteString(href)
			buf.WriteString("\">")
			buf.WriteString(name)
			buf.WriteString("</a></li>")
		}
	}
	if truncated {
		buf.WriteString("<li><em>Listing truncated.</em></li>")
	}

	buf.WriteString("</ul><hr></body></html>")

	c.Type("text/html; charset=utf-8")
	return c.SendBytes(buf.Bytes())
}

func compareFolded(a, b string) int {
	for len(a) > 0 && len(b) > 0 {
		ra, sizeA := utf8.DecodeRuneInString(a)
		rb, sizeB := utf8.DecodeRuneInString(b)
		ra = unicode.ToLower(ra)
		rb = unicode.ToLower(rb)
		if ra < rb {
			return -1
		}
		if ra > rb {
			return 1
		}
		a = a[sizeA:]
		b = b[sizeB:]
	}
	if len(a) == 0 {
		if len(b) == 0 {
			return 0
		}
		return -1
	}
	return 1
}

func readDirBounded(rootFS *os.Root, dir string, limit int) ([]fs.DirEntry, bool, error) {
	if limit <= 0 {
		limit = defaultMaxBrowseEntries
	} else if limit > maxBrowseEntries {
		limit = maxBrowseEntries
	}
	file, err := rootFS.Open(dir)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	entries := make([]fs.DirEntry, 0, min(limit, 128))
	for len(entries) <= limit {
		batchSize := min(128, limit+1-len(entries))
		batch, readErr := file.ReadDir(batchSize)
		if len(batch) == 0 && readErr == nil {
			return nil, false, fmt.Errorf("static: directory read made no progress")
		}
		remaining := limit + 1 - len(entries)
		if len(batch) > remaining {
			batch = batch[:remaining]
		}
		entries = append(entries, batch...)
		if len(entries) > limit {
			return entries[:limit], true, nil
		}
		if readErr != nil {
			if readErr == io.EOF {
				return entries, false, nil
			}
			return nil, false, readErr
		}
	}
	return entries, false, nil
}
