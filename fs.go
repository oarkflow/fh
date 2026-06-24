package fh

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const indexFileName = "index.html"

// DefaultStaticConfig is the default configuration for Static and StaticFS.
var DefaultStaticConfig = StaticConfig{
	Index: "index.html",
}

// StaticConfig configures static file serving behavior.
type StaticConfig struct {
	// Compress enables gzip compression for text-like responses.
	Compress bool

	// MaxAge controls the Cache-Control max-age directive in seconds.
	// Zero omits the header.
	MaxAge int

	// Browse enables directory listing when no index file is found.
	Browse bool

	// Index is the filename used as a directory index. Default: "index.html".
	Index string

	// CacheDuration limits how long file metadata is cached in memory.
	// Zero disables caching.
	CacheDuration time.Duration

	// StripSlash strips the trailing slash from the request path before
	// resolving the file. When true, both /dir and /dir/ serve the same
	// content without a redirect.
	StripSlash bool
}

func (c *StaticConfig) indexFile() string {
	if c.Index != "" {
		return c.Index
	}
	return indexFileName
}

// ── App methods ─────────────────────────────────────────────────────────────

// Static registers a GET route that serves files from root on disk.
func (a *App) Static(prefix, root string, config ...StaticConfig) *App {
	cfg := DefaultStaticConfig
	if len(config) > 0 {
		cfg = config[0]
	}
	a.addStatic(prefix, os.DirFS(root), cfg)
	return a
}

// StaticFS registers a GET route that serves files from an fs.FS (embed.FS, etc.).
func (a *App) StaticFS(prefix string, filesystem fs.FS, config ...StaticConfig) *App {
	cfg := DefaultStaticConfig
	if len(config) > 0 {
		cfg = config[0]
	}
	a.addStatic(prefix, filesystem, cfg)
	return a
}

func (a *App) addStatic(prefix string, filesystem fs.FS, cfg StaticConfig) {
	fsc := &staticFS{
		fs:    filesystem,
		cfg:   cfg,
		cache: newFileCache(cfg.CacheDuration),
	}

	cleanPrefix := "/" + strings.TrimLeft(prefix, "/")

	if cleanPrefix != "/" {
		a.Get(strings.TrimRight(cleanPrefix, "/")+"/*", fsc.serve)
		if cfg.StripSlash {
			a.Get(cleanPrefix, fsc.serve)
		} else {
			a.Get(cleanPrefix, func(c Ctx) error {
				if strings.HasSuffix(c.Path(), "/") {
					return fsc.serve(c)
				}
				return c.Redirect(cleanPrefix+"/", 301)
			})
		}
	} else {
		a.Get("/", fsc.serve)
		a.Get("/*", fsc.serve)
	}
}

// ── Group methods ───────────────────────────────────────────────────────────

// Static registers a GET route that serves files from root on disk.
func (g *Group) Static(prefix, root string, config ...StaticConfig) *Group {
	cfg := DefaultStaticConfig
	if len(config) > 0 {
		cfg = config[0]
	}
	g.addStatic(prefix, os.DirFS(root), cfg)
	return g
}

// StaticFS registers a GET route that serves files from an fs.FS (embed.FS, etc.).
func (g *Group) StaticFS(prefix string, filesystem fs.FS, config ...StaticConfig) *Group {
	cfg := DefaultStaticConfig
	if len(config) > 0 {
		cfg = config[0]
	}
	g.addStatic(prefix, filesystem, cfg)
	return g
}

func (g *Group) addStatic(prefix string, filesystem fs.FS, cfg StaticConfig) {
	fsc := &staticFS{
		fs:    filesystem,
		cfg:   cfg,
		cache: newFileCache(cfg.CacheDuration),
	}

	cleanPrefix := "/" + strings.TrimLeft(prefix, "/")
	fullPrefix := g.prefix + cleanPrefix

	if cleanPrefix != "/" {
		g.Get(strings.TrimRight(cleanPrefix, "/")+"/*", fsc.serve)
		if cfg.StripSlash {
			g.Get(cleanPrefix, fsc.serve)
		} else {
			g.Get(cleanPrefix, func(c Ctx) error {
				if strings.HasSuffix(c.Path(), "/") {
					return fsc.serve(c)
				}
				return c.Redirect(fullPrefix+"/", 301)
			})
		}
	} else {
		g.Get("/", fsc.serve)
		g.Get("/*", fsc.serve)
	}
}

// ── Static file server ──────────────────────────────────────────────────────

type staticFS struct {
	fs    fs.FS
	cfg   StaticConfig
	cache *fileCache
}

var fsGzipPool = sync.Pool{
	New: func() any {
		w, _ := gzip.NewWriterLevel(nil, gzip.BestSpeed)
		return w
	},
}

const httpTimeFormat = "Mon, 02 Jan 2006 15:04:05 GMT"

func (s *staticFS) serve(c Ctx) error {
	upath := c.Param("*")
	if upath == "" {
		return s.servePath(c, ".")
	}
	upath = path.Clean("/" + upath)
	if upath == "/" {
		return s.servePath(c, ".")
	}
	return s.servePath(c, upath[1:])
}

func (s *staticFS) servePath(c Ctx, upath string) error {
	info, err := fs.Stat(s.fs, upath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return c.Status(404).SendString("404 Not Found")
		}
		return c.Status(500).SendString("Internal Server Error")
	}

	if info.IsDir() {
		return s.serveDir(c, upath)
	}
	return s.writeFile(c, upath, info)
}

func (s *staticFS) serveDir(c Ctx, upath string) error {
	if !s.cfg.StripSlash && !strings.HasSuffix(c.Path(), "/") {
		return c.Redirect(c.Path()+"/", 301)
	}

	indexPath := path.Join(upath, s.cfg.indexFile())
	indexInfo, err := fs.Stat(s.fs, indexPath)
	if err == nil && !indexInfo.IsDir() {
		return s.writeFile(c, indexPath, indexInfo)
	}

	if s.cfg.Browse {
		return s.listDir(c, upath)
	}

	return c.Status(403).SendString("Forbidden")
}

func (s *staticFS) writeFile(c Ctx, upath string, info fs.FileInfo) error {
	fi := s.fileInfo(upath, info)

	if match := c.Get("If-Match"); match != "" && !etagListMatches(match, fi.etag, false) {
		return c.Status(StatusPreconditionFailed).SendStatus(StatusPreconditionFailed)
	}
	if ius := c.Get("If-Unmodified-Since"); ius != "" {
		if t, err := time.Parse(httpTimeFormat, ius); err == nil && info.ModTime().After(t.Add(time.Second)) {
			return c.Status(StatusPreconditionFailed).SendStatus(StatusPreconditionFailed)
		}
	}
	if match := c.Get("If-None-Match"); match != "" {
		if etagListMatches(match, fi.etag, true) {
			if c.Method() == MethodGET || c.Method() == MethodHEAD {
				return c.Status(StatusNotModified).SendStatus(StatusNotModified)
			}
			return c.Status(StatusPreconditionFailed).SendStatus(StatusPreconditionFailed)
		}
	}

	if ims := c.Get("If-Modified-Since"); ims != "" && c.Get("If-None-Match") == "" {
		t, err := time.Parse(httpTimeFormat, ims)
		if err == nil && !info.ModTime().After(t) {
			return c.Status(304).SendStatus(304)
		}
	}

	data, err := fs.ReadFile(s.fs, upath)
	if err != nil {
		return c.Status(500).SendString("Internal Server Error")
	}

	mimeType := mime.TypeByExtension(path.Ext(upath))
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	c.Type(mimeType)

	if s.cfg.MaxAge > 0 {
		c.Set("Cache-Control", "public, max-age="+strconv.Itoa(s.cfg.MaxAge))
	}

	c.Set("Last-Modified", info.ModTime().UTC().Format(httpTimeFormat))

	if fi.etag != "" {
		c.Set("ETag", fi.etag)
	}

	c.Set("Accept-Ranges", "bytes")

	fileSize := len(data)
	if rangeHeader := c.Get("Range"); rangeHeader != "" && !s.cfg.Compress {
		if start, end, ok := parseRange(rangeHeader, fileSize); ok {
			ifRange := c.Get("If-Range")
			if ifRange == "" || ifRange == fi.etag || ifRange == info.ModTime().UTC().Format(httpTimeFormat) {
				data = data[start:end]
				c.Status(206)
				c.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, fileSize))
				return c.SendBytes(data)
			}
		} else if strings.HasPrefix(rangeHeader, "bytes=") {
			c.Set("Content-Range", fmt.Sprintf("bytes */%d", fileSize))
			return c.Status(416).SendStatus(416)
		}
	}

	if s.cfg.Compress && isCompressible(mimeType) {
		ae := c.Get("Accept-Encoding")
		if acceptsGzip(ae) {
			c.Set("Content-Encoding", "gzip")
			c.Append("Vary", "Accept-Encoding")
			c.AddBodyTransform(func(body []byte) ([]byte, error) {
				var buf bytes.Buffer
				w := fsGzipPool.Get().(*gzip.Writer)
				w.Reset(&buf)
				_, _ = w.Write(body)
				_ = w.Close()
				w.Reset(io.Discard)
				fsGzipPool.Put(w)
				return buf.Bytes(), nil
			})
		}
	}

	return c.SendBytes(data)
}

// parseRange parses a single Range header value in the form "bytes=start-end".
// Returns (start, end, ok) where end is exclusive. If end is omitted, the
// range extends to the end of the file. If start is omitted (e.g. "bytes=-500"),
// it represents the last N bytes.
func parseRange(header string, fileSize int) (start, end int, ok bool) {
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false
	}
	rangeVal := header[6:]
	if rangeVal == "" {
		return 0, 0, false
	}
	dash := strings.IndexByte(rangeVal, '-')
	if dash < 0 {
		return 0, 0, false
	}
	startStr := rangeVal[:dash]
	endStr := rangeVal[dash+1:]
	if startStr == "" && endStr == "" {
		return 0, 0, false
	}
	if startStr == "" {
		n, err := strconv.Atoi(endStr)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		if n > fileSize {
			n = fileSize
		}
		return fileSize - n, fileSize, true
	}
	start, err := strconv.Atoi(startStr)
	if err != nil || start < 0 || start >= fileSize {
		return 0, 0, false
	}
	if endStr == "" {
		return start, fileSize, true
	}
	end, err = strconv.Atoi(endStr)
	if err != nil || end < start {
		return 0, 0, false
	}
	if end >= fileSize {
		end = fileSize
	}
	return start, end + 1, true
}

func isCompressible(mimeType string) bool {
	if strings.HasPrefix(mimeType, "text/") {
		return true
	}
	switch mimeType {
	case "application/json",
		"application/javascript",
		"application/x-javascript",
		"application/xml",
		"application/xhtml+xml",
		"image/svg+xml":
		return true
	}
	return false
}

// ── Directory listing ───────────────────────────────────────────────────────

func (s *staticFS) listDir(c Ctx, upath string) error {
	entries, err := fs.ReadDir(s.fs, upath)
	if err != nil {
		return c.Status(500).SendString("Internal Server Error")
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].IsDir() != entries[j].IsDir() {
			return entries[i].IsDir()
		}
		return strings.ToLower(entries[i].Name()) < strings.ToLower(entries[j].Name())
	})

	var buf bytes.Buffer
	buf.WriteString("<!DOCTYPE html><html><head><meta charset=\"utf-8\">")
	buf.WriteString("<title>Index of ")
	buf.WriteString(upath)
	buf.WriteString("</title></head><body>")
	buf.WriteString("<h1>Index of ")
	buf.WriteString(upath)
	buf.WriteString("</h1><hr><ul>")

	if upath != "." {
		buf.WriteString("<li><a href=\"../\">../</a></li>")
	}

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() {
			buf.WriteString("<li><a href=\"")
			buf.WriteString(name)
			buf.WriteString("/\"><strong>")
			buf.WriteString(name)
			buf.WriteString("/</strong></a></li>")
		} else {
			buf.WriteString("<li><a href=\"")
			buf.WriteString(name)
			buf.WriteString("\">")
			buf.WriteString(name)
			buf.WriteString("</a></li>")
		}
	}

	buf.WriteString("</ul><hr></body></html>")

	c.Type("text/html; charset=utf-8")
	return c.SendBytes(buf.Bytes())
}

// ── ETag / Info caching ─────────────────────────────────────────────────────

type fileCache struct {
	mu      sync.RWMutex
	entries map[string]cacheEntry
	ttl     time.Duration
}

type cacheEntry struct {
	etag    string
	modTime time.Time
}

func newFileCache(ttl time.Duration) *fileCache {
	if ttl <= 0 {
		return nil
	}
	return &fileCache{
		entries: make(map[string]cacheEntry),
		ttl:     ttl,
	}
}

func (c *fileCache) get(key string) (cacheEntry, bool) {
	if c == nil {
		return cacheEntry{}, false
	}
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()
	return e, ok
}

func (c *fileCache) set(key string, entry cacheEntry) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.entries[key] = entry
	c.mu.Unlock()
}

func generateETag(info fs.FileInfo) string {
	mtime := info.ModTime().UnixNano()
	size := info.Size()

	const hex = "0123456789abcdef"
	var b [35]byte
	b[0] = '"'
	for i := 16; i > 0; i-- {
		b[i] = hex[mtime&0xf]
		mtime >>= 4
	}
	b[17] = '-'
	for i := 33; i > 17; i-- {
		b[i] = hex[size&0xf]
		size >>= 4
	}
	b[34] = '"'
	return string(b[:])
}

func etagListMatches(header, current string, weak bool) bool {
	for _, raw := range strings.Split(header, ",") {
		tag := strings.TrimSpace(raw)
		if tag == "*" {
			return true
		}
		if weak {
			tag = strings.TrimPrefix(tag, "W/")
			cur := strings.TrimPrefix(current, "W/")
			if tag == cur {
				return true
			}
		} else if tag == current {
			return true
		}
	}
	return false
}

func (s *staticFS) fileInfo(upath string, info fs.FileInfo) cacheEntry {
	if e, ok := s.cache.get(upath); ok {
		if e.modTime.Equal(info.ModTime()) {
			return e
		}
	}

	e := cacheEntry{
		etag:    generateETag(info),
		modTime: info.ModTime(),
	}
	s.cache.set(upath, e)
	return e
}

// ── Gzip detection ──────────────────────────────────────────────────────────

func acceptsGzip(header string) bool {
	if header == "" {
		return false
	}
	i := 0
	for i < len(header) {
		for i < len(header) && (header[i] == ',' || header[i] == ' ') {
			i++
		}
		if i >= len(header) {
			break
		}
		start := i
		for i < len(header) && header[i] != ',' && header[i] != ' ' && header[i] != ';' {
			i++
		}
		token := header[start:i]

		qZero := false
		for i < len(header) && header[i] != ',' {
			if header[i] == ';' {
				j := i + 1
				for j < len(header) && header[j] == ' ' {
					j++
				}
				if j+3 <= len(header) && (header[j] == 'q' || header[j] == 'Q') && header[j+1] == '=' && header[j+2] == '0' {
					qZero = true
				}
			}
			i++
		}

		if !qZero {
			if strings.EqualFold(token, "gzip") {
				return true
			}
			if len(token) == 1 && token[0] == '*' {
				return true
			}
		}
	}
	return false
}
