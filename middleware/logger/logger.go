package logger

import (
	"bytes"
	"context"
	"io"
	"log"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
)

type Config struct {
	// Format is used when FormatName is empty or unknown.
	//
	// Supported tokens:
	// ${time} ${ip} ${method} ${path} ${query} ${uri} ${status} ${latency} ${error}
	Format string

	// FormatName can be:
	// default, common, combined, tiny, json
	FormatName string

	// TimeFormat defaults to time.RFC3339.
	TimeFormat string

	// Logger enables standard library log.Logger output.
	// If nil and Slog is nil and Writers is empty, log.Default() is used.
	Logger *log.Logger

	// Slog enables structured slog output.
	Slog *slog.Logger

	// SlogLevel defaults to slog.LevelInfo.
	SlogLevel slog.Level

	// Writers receives rendered text/json log lines.
	// Writers are called only from the async worker.
	Writers []io.Writer

	// QueueSize defaults to 4096.
	QueueSize int

	// MaxLineBytes caps one rendered log line.
	// Defaults to 4096. Larger logs are truncated.
	MaxLineBytes int

	// DropPolicy controls behavior when async queue is full.
	// Default: DropNewest.
	DropPolicy DropPolicy

	// Skip allows custom skipping before logging.
	// It runs before ctx.Next(). If true, no log is written.
	Skip func(*fh.Ctx) bool

	// SkipAfter allows custom skipping after ctx.Next().
	// Useful for status-code based rules.
	SkipAfter func(*fh.Ctx, error) bool

	// SkipDefaultStatic skips common static asset types.
	// Defaults to true.
	SkipDefaultStatic bool

	// SkipExtensions are matched case-insensitively.
	// Example: []string{".css", ".js", ".png"}
	SkipExtensions []string

	// SkipPaths are exact path matches.
	SkipPaths []string

	// SkipPrefixes are prefix path matches.
	SkipPrefixes []string

	// SkipMethods are exact method matches.
	SkipMethods []string

	// SkipStatusCodes skips after request handling.
	SkipStatusCodes []int

	// IncludeQueryInPath makes ${path} include query string.
	IncludeQueryInPath bool

	// DisableAsync is not recommended.
	// If true, writes are performed in request path.
	DisableAsync bool
}

type DropPolicy uint8

const (
	// DropNewest drops the current log entry when queue is full.
	DropNewest DropPolicy = iota

	// DropOldest removes one queued entry and inserts the current one.
	DropOldest
)

const (
	formatDefault  = "[${ip}] ${method} ${path} → ${status} (${latency})\n"
	formatCommon   = `${ip} - - [${time}] "${method} ${uri}" ${status} ${latency}` + "\n"
	formatCombined = `${ip} - - [${time}] "${method} ${uri}" ${status} ${latency}` + "\n"
	formatTiny     = "${method} ${path} ${status} ${latency}\n"
)

var defaultStaticExtensions = []string{
	".css", ".js", ".mjs", ".map",
	".png", ".jpg", ".jpeg", ".gif", ".webp", ".ico", ".svg", ".bmp", ".avif",
	".woff", ".woff2", ".ttf", ".otf", ".eot",
	".mp4", ".webm", ".mp3", ".wav", ".ogg",
	".pdf", ".zip", ".gz", ".br",
}

type Middleware struct {
	cfg       Config
	tokens    []logToken
	json      bool
	output    *asyncOutput
	skip      skipMatcher
	slogOn    bool
	textOn    bool
	closeOnce sync.Once
}

func New(config ...Config) fh.HandlerFunc {
	m := NewMiddleware(config...)
	return m.Handler()
}

func NewMiddleware(config ...Config) *Middleware {
	cfg := Config{
		Format:            formatDefault,
		TimeFormat:        time.RFC3339,
		QueueSize:         4096,
		MaxLineBytes:      4096,
		DropPolicy:        DropNewest,
		SkipDefaultStatic: true,
		SlogLevel:         slog.LevelInfo,
	}

	if len(config) > 0 {
		user := config[0]

		if user.Format != "" {
			cfg.Format = user.Format
		}
		cfg.FormatName = user.FormatName

		if user.TimeFormat != "" {
			cfg.TimeFormat = user.TimeFormat
		}
		if user.Logger != nil {
			cfg.Logger = user.Logger
		}
		if user.Slog != nil {
			cfg.Slog = user.Slog
		}
		cfg.SlogLevel = user.SlogLevel

		if len(user.Writers) > 0 {
			cfg.Writers = user.Writers
		}
		if user.QueueSize > 0 {
			cfg.QueueSize = user.QueueSize
		}
		if user.MaxLineBytes > 0 {
			cfg.MaxLineBytes = user.MaxLineBytes
		}
		cfg.DropPolicy = user.DropPolicy

		cfg.Skip = user.Skip
		cfg.SkipAfter = user.SkipAfter

		cfg.SkipDefaultStatic = user.SkipDefaultStatic
		if !user.SkipDefaultStatic && len(user.SkipExtensions) == 0 {
			cfg.SkipDefaultStatic = false
		}

		cfg.SkipExtensions = user.SkipExtensions
		cfg.SkipPaths = user.SkipPaths
		cfg.SkipPrefixes = user.SkipPrefixes
		cfg.SkipMethods = user.SkipMethods
		cfg.SkipStatusCodes = user.SkipStatusCodes
		cfg.IncludeQueryInPath = user.IncludeQueryInPath
		cfg.DisableAsync = user.DisableAsync
	}

	switch strings.ToLower(cfg.FormatName) {
	case "default":
		cfg.Format = formatDefault
	case "common":
		cfg.Format = formatCommon
	case "combined":
		cfg.Format = formatCombined
	case "tiny":
		cfg.Format = formatTiny
	case "json":
		cfg.Format = ""
	}

	if cfg.TimeFormat == "" {
		cfg.TimeFormat = time.RFC3339
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 4096
	}
	if cfg.MaxLineBytes <= 0 {
		cfg.MaxLineBytes = 4096
	}

	textOn := len(cfg.Writers) > 0 || cfg.Logger != nil || cfg.Slog == nil
	slogOn := cfg.Slog != nil

	if cfg.Logger == nil && cfg.Slog == nil && len(cfg.Writers) == 0 {
		cfg.Logger = log.Default()
	}

	if len(cfg.Writers) == 0 && cfg.Logger == nil && cfg.Slog == nil {
		cfg.Writers = []io.Writer{os.Stderr}
	}

	m := &Middleware{
		cfg:    cfg,
		json:   strings.EqualFold(cfg.FormatName, "json"),
		skip:   newSkipMatcher(cfg),
		slogOn: slogOn,
		textOn: textOn,
	}

	if !m.json {
		m.tokens = parseLogFormat(cfg.Format)
	}

	m.output = newAsyncOutput(cfg, textOn, slogOn)
	return m
}

func (m *Middleware) Handler() fh.HandlerFunc {
	return func(ctx *fh.Ctx) error {
		if m.cfg.Skip != nil && m.cfg.Skip(ctx) {
			return ctx.Next()
		}

		method := ctx.Header.Method
		uri := ctx.Header.URI

		if m.skip.pre(method, uri) {
			return ctx.Next()
		}

		start := time.Now()
		err := ctx.Next()
		end := time.Now()
		lat := end.Sub(start)
		status := ctx.StatusCode()

		if m.skip.post(status) {
			return err
		}
		if m.cfg.SkipAfter != nil && m.cfg.SkipAfter(ctx, err) {
			return err
		}

		path := uriPath(uri, m.cfg.IncludeQueryInPath)
		query := uriQuery(uri)
		ip := ctx.IP()

		var errText string
		if err != nil {
			errText = err.Error()
		}

		if m.textOn {
			buf := renderBufPool.Get().(*bytes.Buffer)
			buf.Reset()

			if m.json {
				m.renderJSON(buf, end, ip, method, path, query, uri, status, lat, errText)
			} else {
				m.renderTokens(buf, end, ip, method, path, query, uri, status, lat, errText)
			}

			if m.cfg.MaxLineBytes > 0 && buf.Len() > m.cfg.MaxLineBytes {
				b := buf.Bytes()
				buf.Reset()
				buf.Write(b[:m.cfg.MaxLineBytes])
				if m.cfg.MaxLineBytes >= 4 {
					buf.Truncate(m.cfg.MaxLineBytes - 4)
					buf.WriteString("...\n")
				}
			}

			m.output.submitLine(buf.Bytes())
			buf.Reset()
			renderBufPool.Put(buf)
		}

		if m.slogOn {
			rec := slogRecordPool.Get().(*slogRecord)
			rec.Time = end
			rec.Method = string(method)
			rec.Path = string(path)
			rec.URI = string(uri)
			rec.Query = string(query)
			rec.IP = ip
			rec.Status = status
			rec.Latency = lat
			rec.Error = errText

			m.output.submitRecord(rec)
		}

		return err
	}
}

func (m *Middleware) Close() error {
	var err error
	m.closeOnce.Do(func() {
		if m.output != nil {
			err = m.output.Close()
		}
	})
	return err
}

func (m *Middleware) Dropped() uint64 {
	if m.output == nil {
		return 0
	}
	return m.output.Dropped()
}

func (m *Middleware) renderTokens(
	buf *bytes.Buffer,
	now time.Time,
	ip string,
	method []byte,
	path []byte,
	query []byte,
	uri []byte,
	status int,
	latency time.Duration,
	errText string,
) {
	for _, t := range m.tokens {
		switch t.typ {
		case logText:
			buf.WriteString(t.text)
		case logTime:
			buf.WriteString(now.Format(m.cfg.TimeFormat))
		case logMethod:
			buf.Write(method)
		case logPath:
			buf.Write(path)
		case logQuery:
			buf.Write(query)
		case logURI:
			buf.Write(uri)
		case logStatus:
			appendInt(buf, status)
		case logLatency:
			appendLatency(buf, latency)
		case logIP:
			buf.WriteString(ip)
		case logError:
			buf.WriteString(errText)
		}
	}
}

func (m *Middleware) renderJSON(
	buf *bytes.Buffer,
	now time.Time,
	ip string,
	method []byte,
	path []byte,
	query []byte,
	uri []byte,
	status int,
	latency time.Duration,
	errText string,
) {
	buf.WriteByte('{')

	writeJSONKeyString(buf, "time")
	writeJSONString(buf, now.Format(m.cfg.TimeFormat))

	buf.WriteByte(',')
	writeJSONKeyString(buf, "ip")
	writeJSONString(buf, ip)

	buf.WriteByte(',')
	writeJSONKeyBytes(buf, "method", method)

	buf.WriteByte(',')
	writeJSONKeyBytes(buf, "path", path)

	if len(query) > 0 {
		buf.WriteByte(',')
		writeJSONKeyBytes(buf, "query", query)
	}

	buf.WriteByte(',')
	writeJSONKeyBytes(buf, "uri", uri)

	buf.WriteByte(',')
	writeJSONKeyString(buf, "status")
	appendInt(buf, status)

	buf.WriteByte(',')
	writeJSONKeyString(buf, "latency_us")
	appendInt64(buf, latency.Microseconds())

	if errText != "" {
		buf.WriteByte(',')
		writeJSONKeyString(buf, "error")
		writeJSONString(buf, errText)
	}

	buf.WriteByte('}')
	buf.WriteByte('\n')
}

type asyncOutput struct {
	cfg     Config
	textOn  bool
	slogOn  bool
	ch      chan *logEntry
	pool    sync.Pool
	done    chan struct{}
	closed  atomic.Bool
	dropped atomic.Uint64
	wg      sync.WaitGroup
}

type logEntry struct {
	line []byte
	rec  *slogRecord
	kind uint8
}

const (
	entryLine uint8 = iota + 1
	entrySlog
)

func newAsyncOutput(cfg Config, textOn bool, slogOn bool) *asyncOutput {
	o := &asyncOutput{
		cfg:    cfg,
		textOn: textOn,
		slogOn: slogOn,
		ch:     make(chan *logEntry, cfg.QueueSize),
		done:   make(chan struct{}),
	}
	o.pool.New = func() any {
		return &logEntry{
			line: make([]byte, 0, 512),
		}
	}

	if !cfg.DisableAsync {
		o.wg.Add(1)
		go o.run()
	}

	return o
}

func (o *asyncOutput) submitLine(line []byte) {
	if len(line) == 0 || o.closed.Load() {
		return
	}

	e := o.pool.Get().(*logEntry)
	e.kind = entryLine
	e.rec = nil
	e.line = append(e.line[:0], line...)

	if o.cfg.DisableAsync {
		o.writeEntry(e)
		o.release(e)
		return
	}

	o.enqueue(e)
}

func (o *asyncOutput) submitRecord(rec *slogRecord) {
	if rec == nil || o.closed.Load() {
		if rec != nil {
			releaseSlogRecord(rec)
		}
		return
	}

	e := o.pool.Get().(*logEntry)
	e.kind = entrySlog
	e.line = e.line[:0]
	e.rec = rec

	if o.cfg.DisableAsync {
		o.writeEntry(e)
		o.release(e)
		return
	}

	o.enqueue(e)
}

func (o *asyncOutput) enqueue(e *logEntry) {
	select {
	case o.ch <- e:
		return
	default:
	}

	o.dropped.Add(1)

	if o.cfg.DropPolicy == DropOldest {
		select {
		case old := <-o.ch:
			o.release(old)
		default:
		}

		select {
		case o.ch <- e:
			return
		default:
			o.dropped.Add(1)
		}
	}

	o.release(e)
}

func (o *asyncOutput) run() {
	defer o.wg.Done()

	for {
		select {
		case e := <-o.ch:
			o.writeEntry(e)
			o.release(e)

		case <-o.done:
			for {
				select {
				case e := <-o.ch:
					o.writeEntry(e)
					o.release(e)
				default:
					return
				}
			}
		}
	}
}

func (o *asyncOutput) writeEntry(e *logEntry) {
	switch e.kind {
	case entryLine:
		if o.textOn {
			if len(o.cfg.Writers) > 0 {
				for _, w := range o.cfg.Writers {
					if w != nil {
						_, _ = w.Write(e.line)
					}
				}
			}
			if o.cfg.Logger != nil {
				_ = o.cfg.Logger.Output(3, string(bytes.TrimRight(e.line, "\n")))
			}
		}

	case entrySlog:
		if o.slogOn && o.cfg.Slog != nil && e.rec != nil {
			attrs := []slog.Attr{
				slog.String("ip", e.rec.IP),
				slog.String("method", e.rec.Method),
				slog.String("path", e.rec.Path),
				slog.String("uri", e.rec.URI),
				slog.Int("status", e.rec.Status),
				slog.Duration("latency", e.rec.Latency),
			}

			if e.rec.Query != "" {
				attrs = append(attrs, slog.String("query", e.rec.Query))
			}
			if e.rec.Error != "" {
				attrs = append(attrs, slog.String("error", e.rec.Error))
			}

			o.cfg.Slog.LogAttrs(context.Background(), o.cfg.SlogLevel, "http_request", attrs...)
		}
	}
}

func (o *asyncOutput) release(e *logEntry) {
	if e == nil {
		return
	}

	if e.rec != nil {
		releaseSlogRecord(e.rec)
		e.rec = nil
	}

	e.kind = 0
	if cap(e.line) > 8192 {
		e.line = make([]byte, 0, 512)
	} else {
		e.line = e.line[:0]
	}
	o.pool.Put(e)
}

func (o *asyncOutput) Close() error {
	if o.closed.Swap(true) {
		return nil
	}

	if !o.cfg.DisableAsync {
		close(o.done)
		o.wg.Wait()
	}

	for _, w := range o.cfg.Writers {
		if c, ok := w.(interface{ Flush() error }); ok {
			_ = c.Flush()
		}
		if c, ok := w.(io.Closer); ok {
			_ = c.Close()
		}
	}

	return nil
}

func (o *asyncOutput) Dropped() uint64 {
	return o.dropped.Load()
}

type slogRecord struct {
	Time    time.Time
	Method  string
	Path    string
	URI     string
	Query   string
	IP      string
	Status  int
	Latency time.Duration
	Error   string
}

var slogRecordPool = sync.Pool{
	New: func() any { return new(slogRecord) },
}

func releaseSlogRecord(r *slogRecord) {
	*r = slogRecord{}
	slogRecordPool.Put(r)
}

type skipMatcher struct {
	extensions []string
	paths      []string
	prefixes   []string
	methods    []string
	statuses   []int
}

func newSkipMatcher(cfg Config) skipMatcher {
	exts := make([]string, 0, len(cfg.SkipExtensions)+len(defaultStaticExtensions))

	if cfg.SkipDefaultStatic {
		exts = append(exts, defaultStaticExtensions...)
	}

	exts = append(exts, cfg.SkipExtensions...)

	for i := range exts {
		exts[i] = normalizeExt(exts[i])
	}

	methods := make([]string, 0, len(cfg.SkipMethods))
	for _, m := range cfg.SkipMethods {
		if m != "" {
			methods = append(methods, strings.ToUpper(m))
		}
	}

	return skipMatcher{
		extensions: exts,
		paths:      cfg.SkipPaths,
		prefixes:   cfg.SkipPrefixes,
		methods:    methods,
		statuses:   cfg.SkipStatusCodes,
	}
}

func (s skipMatcher) pre(method []byte, uri []byte) bool {
	if len(s.methods) > 0 {
		for _, m := range s.methods {
			if asciiEqualBytesString(method, m) {
				return true
			}
		}
	}

	path := uriPath(uri, false)

	if len(s.paths) > 0 {
		for _, p := range s.paths {
			if bytes.Equal(path, unsafeStringBytes(p)) {
				return true
			}
		}
	}

	if len(s.prefixes) > 0 {
		for _, p := range s.prefixes {
			if bytes.HasPrefix(path, unsafeStringBytes(p)) {
				return true
			}
		}
	}

	if len(s.extensions) > 0 && hasAnyExt(path, s.extensions) {
		return true
	}

	return false
}

func (s skipMatcher) post(status int) bool {
	if len(s.statuses) == 0 {
		return false
	}
	for _, v := range s.statuses {
		if v == status {
			return true
		}
	}
	return false
}

func normalizeExt(ext string) string {
	if ext == "" {
		return ext
	}
	if ext[0] != '.' {
		ext = "." + ext
	}
	return strings.ToLower(ext)
}

func hasAnyExt(path []byte, exts []string) bool {
	dot := -1
	for i := len(path) - 1; i >= 0; i-- {
		switch path[i] {
		case '.':
			dot = i
			i = -1
		case '/':
			return false
		}
	}

	if dot < 0 {
		return false
	}

	got := path[dot:]
	for _, ext := range exts {
		if asciiEqualBytesString(got, ext) {
			return true
		}
	}

	return false
}

func uriPath(uri []byte, includeQuery bool) []byte {
	if includeQuery {
		return uri
	}
	if q := bytes.IndexByte(uri, '?'); q >= 0 {
		return uri[:q]
	}
	return uri
}

func uriQuery(uri []byte) []byte {
	if q := bytes.IndexByte(uri, '?'); q >= 0 && q+1 < len(uri) {
		return uri[q+1:]
	}
	return nil
}

func asciiEqualBytesString(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}

	for i := 0; i < len(b); i++ {
		c := b[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}

		d := s[i]
		if d >= 'A' && d <= 'Z' {
			d += 'a' - 'A'
		}

		if c != d {
			return false
		}
	}

	return true
}

func unsafeStringBytes(s string) []byte {
	return []byte(s)
}

type logTokenType uint8

const (
	logText logTokenType = iota
	logTime
	logMethod
	logPath
	logQuery
	logURI
	logStatus
	logLatency
	logIP
	logError
)

type logToken struct {
	typ  logTokenType
	text string
}

func parseLogFormat(format string) []logToken {
	tokens := make([]logToken, 0, 16)

	i := 0
	for i < len(format) {
		if format[i] == '$' && i+2 < len(format) && format[i+1] == '{' {
			end := strings.IndexByte(format[i:], '}')
			if end < 0 {
				tokens = append(tokens, logToken{typ: logText, text: format[i:]})
				break
			}

			name := format[i+2 : i+end]

			switch name {
			case "time":
				tokens = append(tokens, logToken{typ: logTime})
			case "method":
				tokens = append(tokens, logToken{typ: logMethod})
			case "path":
				tokens = append(tokens, logToken{typ: logPath})
			case "query":
				tokens = append(tokens, logToken{typ: logQuery})
			case "uri":
				tokens = append(tokens, logToken{typ: logURI})
			case "status":
				tokens = append(tokens, logToken{typ: logStatus})
			case "latency":
				tokens = append(tokens, logToken{typ: logLatency})
			case "ip":
				tokens = append(tokens, logToken{typ: logIP})
			case "error":
				tokens = append(tokens, logToken{typ: logError})
			default:
				tokens = append(tokens, logToken{typ: logText, text: format[i : i+end+1]})
			}

			i += end + 1
			continue
		}

		start := i
		for i < len(format) && !(format[i] == '$' && i+2 < len(format) && format[i+1] == '{') {
			i++
		}

		if start != i {
			tokens = append(tokens, logToken{typ: logText, text: format[start:i]})
		}
	}

	return tokens
}

func appendLatency(buf *bytes.Buffer, d time.Duration) {
	if d < time.Microsecond {
		appendInt64(buf, int64(d))
		buf.WriteString("ns")
		return
	}

	if d < time.Millisecond {
		appendInt64(buf, d.Microseconds())
		buf.WriteString("µs")
		return
	}

	if d < time.Second {
		ms := d.Microseconds()
		appendFixed3(buf, ms)
		buf.WriteString("ms")
		return
	}

	us := d.Microseconds()
	sec := us / 1_000_000
	rem := (us % 1_000_000) / 1000

	appendInt64(buf, sec)
	buf.WriteByte('.')
	if rem < 100 {
		buf.WriteByte('0')
	}
	if rem < 10 {
		buf.WriteByte('0')
	}
	appendInt64(buf, rem)
	buf.WriteByte('s')
}

func appendFixed3(buf *bytes.Buffer, micro int64) {
	ms := micro / 1000
	rem := micro % 1000

	appendInt64(buf, ms)
	buf.WriteByte('.')

	if rem < 100 {
		buf.WriteByte('0')
	}
	if rem < 10 {
		buf.WriteByte('0')
	}

	appendInt64(buf, rem)
}

func appendInt(buf *bytes.Buffer, n int) {
	appendInt64(buf, int64(n))
}

func appendInt64(buf *bytes.Buffer, n int64) {
	if n == 0 {
		buf.WriteByte('0')
		return
	}

	if n < 0 {
		buf.WriteByte('-')
		n = -n
	}

	var s [20]byte
	i := len(s)

	for n > 0 {
		i--
		s[i] = byte('0' + n%10)
		n /= 10
	}

	buf.Write(s[i:])
}

func writeJSONKeyString(buf *bytes.Buffer, key string) {
	buf.WriteByte('"')
	buf.WriteString(key)
	buf.WriteString(`":`)
}

func writeJSONKeyBytes(buf *bytes.Buffer, key string, value []byte) {
	writeJSONKeyString(buf, key)
	writeJSONStringBytes(buf, value)
}

func writeJSONString(buf *bytes.Buffer, s string) {
	buf.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\\', '"':
			buf.WriteByte('\\')
			buf.WriteByte(c)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\t':
			buf.WriteString(`\t`)
		default:
			if c < 0x20 {
				buf.WriteString(`\u00`)
				hex := "0123456789abcdef"
				buf.WriteByte(hex[c>>4])
				buf.WriteByte(hex[c&0x0f])
			} else {
				buf.WriteByte(c)
			}
		}
	}
	buf.WriteByte('"')
}

func writeJSONStringBytes(buf *bytes.Buffer, b []byte) {
	buf.WriteByte('"')
	for i := 0; i < len(b); i++ {
		c := b[i]
		switch c {
		case '\\', '"':
			buf.WriteByte('\\')
			buf.WriteByte(c)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\t':
			buf.WriteString(`\t`)
		default:
			if c < 0x20 {
				buf.WriteString(`\u00`)
				hex := "0123456789abcdef"
				buf.WriteByte(hex[c>>4])
				buf.WriteByte(hex[c&0x0f])
			} else {
				buf.WriteByte(c)
			}
		}
	}
	buf.WriteByte('"')
}

var renderBufPool = sync.Pool{
	New: func() any {
		b := bytes.NewBuffer(make([]byte, 0, 512))
		return b
	},
}
