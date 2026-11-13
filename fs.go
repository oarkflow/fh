package fh

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"html"
	"io"
	"io/fs"
	"mime"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const indexFileName = "index.html"

const streamCompressionThreshold = 64 << 10
const defaultMaxStaticRanges = 16
const defaultMaxStaticBrowseEntries = 1000
const maxStaticBrowseEntriesLimit = 10000
const maxStaticFileCacheEntries = 8192
const maxStaticFileCacheKeyBytes = 4096

// DefaultStaticConfig is the default configuration for Static and StaticFS.
var DefaultStaticConfig = StaticConfig{
	Index:     "index.html",
	MaxRanges: defaultMaxStaticRanges,
}

// StaticConfig configures static file serving behavior.
type StaticConfig struct {
	// Compress enables gzip compression for text-like responses.
	Compress bool

	// MaxAge controls the Cache-Control max-age directive in seconds.
	// Zero omits the header. Superseded by CacheControl when both are set.
	MaxAge int

	// CacheControl, when non-empty, overrides the entire Cache-Control header
	// for all served files. Takes precedence over MaxAge.
	// Example: "public, max-age=86400, immutable"
	CacheControl string

	// Browse enables directory listing when no index file is found.
	Browse bool

	// Index is the filename used as a directory index. Default: "index.html".
	// Superseded by IndexFiles when both are set.
	Index string

	// IndexFiles is an ordered list of filenames tried as the directory index.
	// The first matching file is served. Takes precedence over Index.
	// Example: []string{"index.html", "index.htm", "default.html"}
	IndexFiles []string

	// CacheDuration limits how long file metadata is cached in memory.
	// Zero disables caching.
	CacheDuration time.Duration

	// StripSlash strips the trailing slash from the request path before
	// resolving the file. When true, both /dir and /dir/ serve the same
	// content without a redirect.
	StripSlash bool

	// ShowHidden includes dotfiles (e.g. .env, .git) in directory listings.
	// Defaults to false so accidental secrets/metadata in the served root
	// are not exposed through Browse.
	ShowHidden bool

	// NotFoundHandler, when non-nil, is called instead of returning a 404
	// when the requested file or directory is not found under the static root.
	// Useful for single-page applications: serve index.html for any unknown path.
	NotFoundHandler HandlerFunc

	// PreCompressed enables transparent serving of pre-compressed sidecar files.
	// When the client accepts br (Brotli), fh checks for <path>.br before <path>.
	// When the client accepts gzip, fh checks for <path>.gz before <path>.
	// The original Content-Type is preserved and Content-Encoding is set.
	PreCompressed bool

	// MaxRanges limits the number of byte ranges accepted in one request.
	// Zero uses the safe default of 16 ranges.
	MaxRanges int

	// MaxBrowseEntries limits the number of directory entries rendered when
	// Browse is enabled. Zero uses the safe default of 1000 entries; values
	// above 10000 are clamped. The filesystem must expose fs.ReadDirFile so
	// fh can stop reading at the cap without loading the full directory.
	MaxBrowseEntries int
}

// indexFiles returns the ordered list of index filenames to try.
func (c *StaticConfig) indexFiles() []string {
	if len(c.IndexFiles) > 0 {
		return c.IndexFiles
	}
	if c.Index != "" {
		return []string{c.Index}
	}
	return []string{indexFileName}
}

// ── App methods ─────────────────────────────────────────────────────────────

// Static registers a GET route that serves files from root on disk.
func (a *App) Static(prefix, root string, config ...StaticConfig) *App {
	cfg := DefaultStaticConfig
	if len(config) > 0 {
		cfg = config[0]
	}
	rootFS, err := os.OpenRoot(root)
	if err != nil {
		panic(fmt.Errorf("fh: open static root %q: %w", root, err))
	}
	a.staticRoots = append(a.staticRoots, rootFS)
	a.addStatic(prefix, rootFS.FS(), cfg)
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
	cfg.IndexFiles = append([]string(nil), cfg.IndexFiles...)
	if cfg.MaxRanges <= 0 {
		cfg.MaxRanges = defaultMaxStaticRanges
	}
	if cfg.MaxBrowseEntries <= 0 {
		cfg.MaxBrowseEntries = defaultMaxStaticBrowseEntries
	} else if cfg.MaxBrowseEntries > maxStaticBrowseEntriesLimit {
		cfg.MaxBrowseEntries = maxStaticBrowseEntriesLimit
	}
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
	rootFS, err := os.OpenRoot(root)
	if err != nil {
		panic(fmt.Errorf("fh: open static root %q: %w", root, err))
	}
	g.app.staticRoots = append(g.app.staticRoots, rootFS)
	g.addStatic(prefix, rootFS.FS(), cfg)
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
	cfg.IndexFiles = append([]string(nil), cfg.IndexFiles...)
	if cfg.MaxRanges <= 0 {
		cfg.MaxRanges = defaultMaxStaticRanges
	}
	if cfg.MaxBrowseEntries <= 0 {
		cfg.MaxBrowseEntries = defaultMaxStaticBrowseEntries
	} else if cfg.MaxBrowseEntries > maxStaticBrowseEntriesLimit {
		cfg.MaxBrowseEntries = maxStaticBrowseEntriesLimit
	}
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
	for start := 0; start < len(upath); {
		end := strings.IndexByte(upath[start:], '/')
		if end < 0 {
			end = len(upath) - start
		}
		if upath[start:start+end] == ".." {
			return c.Status(404).SendString("404 Not Found")
		}
		start += end + 1
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
			if s.cfg.NotFoundHandler != nil {
				return s.cfg.NotFoundHandler(c)
			}
			return c.Status(404).SendString("404 Not Found")
		}
		if errors.Is(err, fs.ErrPermission) {
			return c.Status(403).SendString("Forbidden")
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

	for _, idxFile := range s.cfg.indexFiles() {
		indexPath := path.Join(upath, idxFile)
		indexInfo, err := fs.Stat(s.fs, indexPath)
		if err == nil && !indexInfo.IsDir() {
			return s.writeFile(c, indexPath, indexInfo)
		}
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

	mimeType := mime.TypeByExtension(path.Ext(upath))
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	c.Type(mimeType)

	// Cache-Control: CacheControl takes precedence over MaxAge.
	if s.cfg.CacheControl != "" {
		c.Set("Cache-Control", s.cfg.CacheControl)
	} else if s.cfg.MaxAge > 0 {
		c.Set("Cache-Control", "public, max-age="+strconv.Itoa(s.cfg.MaxAge))
	}

	c.Set("Last-Modified", info.ModTime().UTC().Format(httpTimeFormat))

	if fi.etag != "" {
		c.Set("ETag", fi.etag)
	}

	c.Set("Accept-Ranges", "bytes")

	fileSize := info.Size()
	if rangeHeader := c.Get("Range"); rangeHeader != "" && !s.cfg.Compress {
		if strings.HasPrefix(rangeHeader, "bytes=") {
			ranges, rangeErr := parseRangeHeaderLimit(rangeHeader, fileSize, s.cfg.MaxRanges)
			if rangeErr != nil {
				c.Set("Content-Range", fmt.Sprintf("bytes */%d", fileSize))
				return c.Status(416).SendStatus(416)
			}
			ifRange := c.Get("If-Range")
			if ifRange == "" || (!strings.HasPrefix(fi.etag, "W/") && ifRange == fi.etag) || ifRange == info.ModTime().UTC().Format(httpTimeFormat) {
				c.Status(206)
				if len(ranges) == 1 {
					start, end := ranges[0].Start, ranges[0].End
					c.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, fileSize))
					return streamStaticFileRange(c, s.fs, upath, start, end-start+1)
				}
				return streamStaticFileRanges(c, s.fs, upath, mimeType, fileSize, ranges, info.ModTime())
			}
		}
	}

	// Pre-compressed sidecar serving: check for .br / .gz sidecar files
	// before falling back to on-the-fly gzip compression.
	if s.cfg.PreCompressed {
		if encoding := preferredStaticEncoding(c.Get("Accept-Encoding")); encoding != "" {
			suffix := encoding
			if encoding == "gzip" {
				suffix = "gz"
			}
			compressedPath := upath + "." + suffix
			if compressedInfo, err2 := fs.Stat(s.fs, compressedPath); err2 == nil && !compressedInfo.IsDir() {
				c.Set("Content-Encoding", encoding)
				c.Append("Vary", "Accept-Encoding")
				return streamStaticFile(c, s.fs, compressedPath, compressedInfo.Size())
			}
		}
	}

	if s.cfg.Compress && isCompressible(mimeType) {
		ae := c.Get("Accept-Encoding")
		if acceptsGzip(ae) {
			if info.Size() >= streamCompressionThreshold && canStreamStaticResponse(c) {
				c.Set("Content-Encoding", "gzip")
				c.Append("Vary", "Accept-Encoding")
				return streamGzipStaticFile(c, s.fs, upath)
			}
			data, err := fs.ReadFile(s.fs, upath)
			if err != nil {
				return c.Status(500).SendString("Internal Server Error")
			}
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
			return c.SendBytes(data)
		}
	}

	// The common uncompressed path streams directly from the filesystem (or
	// embedded fs) and retains a known Content-Length, avoiding an allocation
	// proportional to the asset size and keeping HTTP/1.1 connections reusable.
	return streamStaticFile(c, s.fs, upath, info.Size())
}

func streamStaticFile(c Ctx, filesystem fs.FS, path string, size int64) error {
	file, err := filesystem.Open(path)
	if err != nil {
		return c.Status(500).SendString("Internal Server Error")
	}
	defer file.Close()
	return c.SendStreamLength(file, size)
}

// streamStaticFileRange streams only the selected byte range. Files that
// implement io.Seeker (including os.File) jump directly to the offset;
// generic fs.FS implementations fall back to discarding the prefix without
// buffering the complete file.
func streamStaticFileRange(c Ctx, filesystem fs.FS, path string, start, size int64) error {
	file, err := filesystem.Open(path)
	if err != nil {
		return c.Status(500).SendString("Internal Server Error")
	}
	defer file.Close()
	if seeker, ok := file.(io.Seeker); ok {
		if _, err := seeker.Seek(start, io.SeekStart); err != nil {
			return c.Status(500).SendString("Internal Server Error")
		}
	} else if start > 0 {
		if _, err := io.CopyN(io.Discard, file, start); err != nil {
			return c.Status(500).SendString("Internal Server Error")
		}
	}
	return c.SendStreamLength(io.LimitReader(file, size), size)
}

// streamStaticFileRanges writes a bounded multipart/byteranges response. The
// file is opened once and each segment is copied directly to the response, so
// even a large multi-range request never materializes the representation.
func streamStaticFileRanges(c Ctx, filesystem fs.FS, path, mimeType string, total int64, ranges []ByteRange, modTime time.Time) error {
	boundary := "fh-range-" + strconv.FormatInt(total, 16) + "-" + strconv.FormatInt(modTime.UnixNano(), 16)
	c.Type("multipart/byteranges; boundary=" + boundary)
	return c.Stream(func(w *StreamWriter) error {
		if w.discard {
			return nil
		}
		file, err := filesystem.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		var scratch [32 << 10]byte
		for _, r := range ranges {
			header := "--" + boundary + "\r\nContent-Type: " + mimeType + "\r\nContent-Range: bytes " + strconv.FormatInt(r.Start, 10) + "-" + strconv.FormatInt(r.End, 10) + "/" + strconv.FormatInt(total, 10) + "\r\n\r\n"
			if _, err := w.Write([]byte(header)); err != nil {
				return err
			}
			if seeker, ok := file.(io.Seeker); ok {
				if _, err := seeker.Seek(r.Start, io.SeekStart); err != nil {
					return err
				}
			} else if r.Start > 0 {
				if _, err := io.CopyN(io.Discard, file, r.Start); err != nil {
					return err
				}
			}
			if _, err := io.CopyBuffer(w, io.LimitReader(file, r.End-r.Start+1), scratch[:]); err != nil {
				return err
			}
			if _, err := w.Write([]byte("\r\n")); err != nil {
				return err
			}
		}
		_, err = w.Write([]byte("--" + boundary + "--\r\n"))
		return err
	})
}

func canStreamStaticResponse(c Ctx) bool {
	dc, ok := c.(*DefaultCtx)
	return ok && dc.bodyTransform == nil && !dc.captureResponseBody
}

func streamGzipStaticFile(c Ctx, filesystem fs.FS, path string) error {
	return c.Stream(func(w *StreamWriter) error {
		if w.discard {
			return nil
		}
		file, err := filesystem.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		gzipWriter := fsGzipPool.Get().(*gzip.Writer)
		gzipWriter.Reset(w)
		_, copyErr := io.Copy(gzipWriter, file)
		closeErr := gzipWriter.Close()
		gzipWriter.Reset(io.Discard)
		fsGzipPool.Put(gzipWriter)
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
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
	entries, truncated, err := readStaticDirEntries(s.fs, upath, s.cfg.MaxBrowseEntries)
	if err != nil {
		return c.Status(500).SendString("Internal Server Error")
	}

	if !s.cfg.ShowHidden {
		visible := entries[:0]
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Name(), ".") {
				visible = append(visible, entry)
			}
		}
		entries = visible
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].IsDir() != entries[j].IsDir() {
			return entries[i].IsDir()
		}
		return strings.ToLower(entries[i].Name()) < strings.ToLower(entries[j].Name())
	})

	// Entry names come from the filesystem and the path comes from the
	// request URL; both are escaped before being written into HTML since
	// either can be attacker-influenced (e.g. a file uploaded elsewhere on
	// the served root with a name like "<script>...").
	safePath := html.EscapeString(upath)

	var buf bytes.Buffer
	buf.WriteString("<!DOCTYPE html><html><head><meta charset=\"utf-8\">")
	buf.WriteString("<title>Index of ")
	buf.WriteString(safePath)
	buf.WriteString("</title></head><body>")
	buf.WriteString("<h1>Index of ")
	buf.WriteString(safePath)
	buf.WriteString("</h1><hr><ul>")

	if upath != "." {
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
		buf.WriteString("<li><em>Listing truncated at ")
		buf.WriteString(strconv.Itoa(s.cfg.MaxBrowseEntries))
		buf.WriteString(" entries.</em></li>")
	}

	buf.WriteString("</ul><hr></body></html>")

	c.Type("text/html; charset=utf-8")
	return c.SendBytes(buf.Bytes())
}

// readStaticDirEntries bounds directory memory use for ReadDirFile-backed
// filesystems by reading in small batches and stopping after limit+1 entries.
// The extra entry lets the caller report that the listing was truncated.
func readStaticDirEntries(filesystem fs.FS, name string, limit int) ([]fs.DirEntry, bool, error) {
	if limit <= 0 {
		limit = defaultMaxStaticBrowseEntries
	} else if limit > maxStaticBrowseEntriesLimit {
		limit = maxStaticBrowseEntriesLimit
	}
	file, err := filesystem.Open(name)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	dir, ok := file.(fs.ReadDirFile)
	if !ok {
		return nil, false, errors.New("fs: directory listing requires a bounded ReadDirFile")
	}
	entries := make([]fs.DirEntry, 0, min(limit, 128))
	for len(entries) <= limit {
		batchSize := min(128, limit+1-len(entries))
		batch, readErr := dir.ReadDir(batchSize)
		if len(batch) == 0 && readErr == nil {
			return nil, false, errors.New("fs: ReadDir returned no entries without an error")
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
			if errors.Is(readErr, io.EOF) {
				return entries, false, nil
			}
			return nil, false, readErr
		}
	}
	return entries, false, nil
}

// ── ETag / Info caching ─────────────────────────────────────────────────────

type fileCache struct {
	mu      sync.RWMutex
	entries map[string]cacheEntry
	ttl     time.Duration
}

type cacheEntry struct {
	etag     string
	modTime  time.Time
	size     int64
	cachedAt time.Time
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
	if c == nil || len(key) > maxStaticFileCacheKeyBytes {
		return cacheEntry{}, false
	}
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok {
		return cacheEntry{}, false
	}
	if time.Since(e.cachedAt) < c.ttl {
		return e, true
	}
	c.mu.Lock()
	if current, exists := c.entries[key]; exists && current.cachedAt.Equal(e.cachedAt) {
		delete(c.entries, key)
	}
	c.mu.Unlock()
	return cacheEntry{}, false
}

func (c *fileCache) set(key string, entry cacheEntry) {
	if c == nil || len(key) > maxStaticFileCacheKeyBytes {
		return
	}
	now := time.Now()
	entry.cachedAt = now
	c.mu.Lock()
	if _, exists := c.entries[key]; !exists && len(c.entries) >= maxStaticFileCacheEntries {
		// Lookups discard expired entries. Evict one entry here instead of
		// scanning the full cache under the write lock on every new path once
		// the cache reaches its cap.
		for candidate := range c.entries {
			delete(c.entries, candidate)
			break
		}
	}
	c.entries[key] = entry
	c.mu.Unlock()
}

func generateETag(info fs.FileInfo) string {
	mtime := info.ModTime().UnixNano()
	size := info.Size()

	const hex = "0123456789abcdef"
	var b [37]byte
	b[0], b[1] = 'W', '/'
	b[2] = '"'
	for i := 18; i > 2; i-- {
		b[i] = hex[mtime&0xf]
		mtime >>= 4
	}
	b[19] = '-'
	for i := 35; i > 19; i-- {
		b[i] = hex[size&0xf]
		size >>= 4
	}
	b[36] = '"'
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
		if e.modTime.Equal(info.ModTime()) && e.size == info.Size() {
			return e
		}
	}

	e := cacheEntry{
		etag:    generateETag(info),
		modTime: info.ModTime(),
		size:    info.Size(),
	}
	s.cache.set(upath, e)
	return e
}

// ── Gzip detection ──────────────────────────────────────────────────────────

func acceptsGzip(header string) bool {
	return encodingQuality(header, "gzip") > 0
}

// preferredStaticEncoding returns the highest-quality supported sidecar
// encoding. Brotli wins ties because it generally produces smaller assets.
// An explicit token always overrides a wildcard, including q=0.
func preferredStaticEncoding(header string) string {
	br := encodingQuality(header, "br")
	gzip := encodingQuality(header, "gzip")
	if br <= 0 && gzip <= 0 {
		return ""
	}
	if br >= gzip {
		return "br"
	}
	return "gzip"
}

func encodingQuality(header, encoding string) float64 {
	if header == "" {
		return 0
	}
	var explicit, wildcard float64
	var hasExplicit, hasWildcard bool
	for _, item := range strings.Split(header, ",") {
		parts := strings.Split(item, ";")
		token := strings.TrimSpace(parts[0])
		if token == "" {
			continue
		}
		quality := 1.0
		for _, parameter := range parts[1:] {
			key, value, ok := strings.Cut(strings.TrimSpace(parameter), "=")
			if !ok || !strings.EqualFold(strings.TrimSpace(key), "q") {
				continue
			}
			parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
			if err != nil || parsed < 0 || parsed > 1 {
				quality = 0
			} else {
				quality = parsed
			}
		}
		if token == "*" {
			wildcard, hasWildcard = quality, true
		} else if strings.EqualFold(token, encoding) {
			explicit, hasExplicit = quality, true
		}
	}
	if hasExplicit {
		return explicit
	}
	if hasWildcard {
		return wildcard
	}
	return 0
}
