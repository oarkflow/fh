package middleware

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fh "github.com/orgware/fasthttp"
)

// ── Logger ─────────────────────────────────────────────────────────────────

// LoggerConfig configures the logger middleware.
type LoggerConfig struct {
	Format string
	Logger *log.Logger
}

type logTokenType uint8

const (
	logText logTokenType = iota
	logMethod
	logPath
	logStatus
	logLatency
	logIP
)

type logToken struct {
	typ  logTokenType
	text string
}

func parseLogFormat(format string) []logToken {
	tokens := make([]logToken, 0, 8)
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
			case "method":
				tokens = append(tokens, logToken{typ: logMethod})
			case "path":
				tokens = append(tokens, logToken{typ: logPath})
			case "status":
				tokens = append(tokens, logToken{typ: logStatus})
			case "latency":
				tokens = append(tokens, logToken{typ: logLatency})
			case "ip":
				tokens = append(tokens, logToken{typ: logIP})
			default:
				tokens = append(tokens, logToken{typ: logText, text: format[i:i+end+1]})
			}
			i += end + 1
		} else {
			start := i
			for i < len(format) && !(format[i] == '$' && i+2 < len(format) && format[i+1] == '{') {
				i++
			}
			tokens = append(tokens, logToken{typ: logText, text: format[start:i]})
		}
	}
	return tokens
}

func appendInt(buf *bytes.Buffer, n int) {
	if n < 1000 {
		switch n {
		case 0:
			buf.WriteByte('0')
			return
		case 1:
			buf.WriteByte('1')
			return
		case 2:
			buf.WriteByte('2')
			return
		case 3:
			buf.WriteByte('3')
			return
		case 4:
			buf.WriteByte('4')
			return
		case 5:
			buf.WriteByte('5')
			return
		case 6:
			buf.WriteByte('6')
			return
		case 7:
			buf.WriteByte('7')
			return
		case 8:
			buf.WriteByte('8')
			return
		case 9:
			buf.WriteByte('9')
			return
		}
	}
	var s [10]byte
	i := len(s)
	for n > 0 || i == len(s) {
		i--
		s[i] = byte('0' + n%10)
		n /= 10
	}
	buf.Write(s[i:])
}

var logBufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

// Logger logs each request. The format string is compiled once at setup time;
// at runtime, output is built with a single pass using a pooled buffer.
func Logger(config ...LoggerConfig) fh.HandlerFunc {
	cfg := LoggerConfig{
		Format: "[${ip}] ${method} ${path} → ${status} (${latency})\n",
	}
	if len(config) > 0 {
		cfg = config[0]
	}
	logger := cfg.Logger
	if logger == nil {
		logger = log.Default()
	}
	tokens := parseLogFormat(cfg.Format)

	return func(ctx *fh.Ctx) error {
		start := time.Now()
		err := ctx.Next()
		lat := time.Since(start)

		buf := logBufPool.Get().(*bytes.Buffer)
		buf.Reset()
		buf.Grow(len(cfg.Format) + 64)

		for _, t := range tokens {
			switch t.typ {
			case logText:
				buf.WriteString(t.text)
			case logMethod:
				buf.Write(ctx.Header.Method)
			case logPath:
				uri := ctx.Header.URI
				if q := bytes.IndexByte(uri, '?'); q >= 0 {
					buf.Write(uri[:q])
				} else {
					buf.Write(uri)
				}
			case logStatus:
				appendInt(buf, ctx.StatusCode())
			case logLatency:
				us := lat.Microseconds()
				var lb [16]byte
				n := len(lb)
				for us > 0 {
					n--
					lb[n] = byte('0' + us%10)
					us /= 10
				}
				if n == len(lb) {
					lb[len(lb)-1] = '0'
					n = len(lb) - 1
				}
				buf.Write(lb[n:])
				buf.WriteString("µs")
			case logIP:
				buf.WriteString(ctx.IP())
			}
		}
		logger.Output(2, buf.String())
		buf.Reset()
		logBufPool.Put(buf)
		return err
	}
}

// ── Recover ────────────────────────────────────────────────────────────────

func Recover() fh.HandlerFunc {
	return func(ctx *fh.Ctx) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic: %v", r)
				ctx.Status(500).SendString("Internal Server Error")
			}
		}()
		return ctx.Next()
	}
}

// ── CORS ───────────────────────────────────────────────────────────────────

type CORSConfig struct {
	AllowOrigins     []string
	AllowMethods     []string
	AllowHeaders     []string
	AllowCredentials bool
	MaxAge           int
}

var defaultCORSConfig = CORSConfig{
	AllowOrigins: []string{"*"},
	AllowMethods: []string{"GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"},
	AllowHeaders: []string{"Content-Type", "Authorization"},
	MaxAge:       86400,
}

func CORS(config ...CORSConfig) fh.HandlerFunc {
	cfg := defaultCORSConfig
	if len(config) > 0 {
		cfg = config[0]
	}

	allowedOrigins := make(map[string]struct{}, len(cfg.AllowOrigins))
	wildcard := false
	for _, origin := range cfg.AllowOrigins {
		if origin == "*" {
			wildcard = true
		} else {
			allowedOrigins[origin] = struct{}{}
		}
	}
	methods := strings.Join(cfg.AllowMethods, ", ")
	headers := strings.Join(cfg.AllowHeaders, ", ")
	maxAge := strconv.Itoa(cfg.MaxAge)

	return func(ctx *fh.Ctx) error {
		origin := ctx.Get("Origin")
		if origin == "" {
			return ctx.Next()
		}
		_, exact := allowedOrigins[origin]
		if !wildcard && !exact {
			return ctx.Next()
		}
		if wildcard && !cfg.AllowCredentials {
			ctx.Set("Access-Control-Allow-Origin", "*")
		} else {
			ctx.Set("Access-Control-Allow-Origin", origin)
			ctx.Append("Vary", "Origin")
		}
		if cfg.AllowCredentials {
			ctx.Set("Access-Control-Allow-Credentials", "true")
		}
		if len(ctx.Header.Method) == 7 &&
			ctx.Header.Method[0] == 'O' &&
			ctx.Header.Method[1] == 'P' &&
			ctx.Header.Method[2] == 'T' &&
			ctx.Header.Method[3] == 'I' &&
			ctx.Header.Method[4] == 'O' &&
			ctx.Header.Method[5] == 'N' &&
			ctx.Header.Method[6] == 'S' && ctx.Get("Access-Control-Request-Method") != "" {
			ctx.Set("Access-Control-Allow-Methods", methods)
			ctx.Set("Access-Control-Allow-Headers", headers)
			ctx.Set("Access-Control-Max-Age", maxAge)
			return ctx.SendStatus(204)
		}
		return ctx.Next()
	}
}

// ── RequestID ──────────────────────────────────────────────────────────────

var ridCounter uint64
var ridPrefix = strconv.FormatInt(time.Now().UnixNano(), 36)

func RequestID() fh.HandlerFunc {
	return func(ctx *fh.Ctx) error {
		id := ctx.Get("X-Request-ID")
		if id == "" {
			n := atomic.AddUint64(&ridCounter, 1)
			var buf [64]byte
			m := copy(buf[:], ridPrefix)
			buf[m] = '-'
			m++
			m += len(strconv.AppendUint(buf[m:m], n, 36))
			id = string(buf[:m])
		}
		ctx.Set("X-Request-ID", id)
		ctx.Locals("requestID", id)
		return ctx.Next()
	}
}

// ── Rate Limiter ───────────────────────────────────────────────────────────

type RateLimiterConfig struct {
	Max          int
	Window       time.Duration
	KeyFunc      func(*fh.Ctx) string
	LimitReached func(*fh.Ctx) error
}

type rateBucket struct {
	mu      sync.Mutex
	count   int
	resetAt time.Time
}

func RateLimiter(config RateLimiterConfig) fh.HandlerFunc {
	if config.Max <= 0 {
		config.Max = 100
	}
	if config.Window <= 0 {
		config.Window = time.Minute
	}
	if config.KeyFunc == nil {
		config.KeyFunc = func(ctx *fh.Ctx) string { return ctx.IP() }
	}
	if config.LimitReached == nil {
		config.LimitReached = func(ctx *fh.Ctx) error {
			ctx.Set("Retry-After", strconv.Itoa(int(config.Window.Seconds())))
			return ctx.Status(429).SendString("Too Many Requests")
		}
	}

	maxStr := strconv.Itoa(config.Max)

	var mu sync.RWMutex
	buckets := make(map[string]*rateBucket, 1024)
	var nextCleanup atomic.Int64
	nextCleanup.Store(time.Now().Add(config.Window).UnixNano())

	return func(ctx *fh.Ctx) error {
		now := time.Now()
		if deadline := nextCleanup.Load(); now.UnixNano() >= deadline && nextCleanup.CompareAndSwap(deadline, now.Add(config.Window).UnixNano()) {
			mu.Lock()
			for k, b := range buckets {
				b.mu.Lock()
				expired := now.After(b.resetAt)
				b.mu.Unlock()
				if expired {
					delete(buckets, k)
				}
			}
			mu.Unlock()
		}
		key := config.KeyFunc(ctx)

		mu.RLock()
		b := buckets[key]
		mu.RUnlock()

		if b == nil {
			mu.Lock()
			b = buckets[key]
			if b == nil {
				b = &rateBucket{resetAt: now.Add(config.Window)}
				buckets[key] = b
			}
			mu.Unlock()
		}

		b.mu.Lock()
		if now.After(b.resetAt) {
			b.count = 0
			b.resetAt = now.Add(config.Window)
		}
		b.count++
		count := b.count
		b.mu.Unlock()

		ctx.Set("X-RateLimit-Limit", maxStr)
		rem := config.Max - count
		if rem < 0 {
			rem = 0
		}
		var remBuf [10]byte
		remStr := string(strconv.AppendInt(remBuf[:0], int64(rem), 10))
		ctx.Set("X-RateLimit-Remaining", remStr)

		if count > config.Max {
			return config.LimitReached(ctx)
		}
		return ctx.Next()
	}
}

// ── Compress ───────────────────────────────────────────────────────────────

var gzipPool = sync.Pool{
	New: func() any {
		w, _ := gzip.NewWriterLevel(nil, gzip.BestSpeed)
		return w
	},
}

func Compress() fh.HandlerFunc {
	return func(ctx *fh.Ctx) error {
		ae := ctx.Get("Accept-Encoding")
		if !acceptsGzip(ae) {
			return ctx.Next()
		}
		ctx.Set("Content-Encoding", "gzip")
		ctx.Append("Vary", "Accept-Encoding")
		ctx.TransformBody(func(body []byte) ([]byte, error) {
			var dst bytes.Buffer
			w := gzipPool.Get().(*gzip.Writer)
			w.Reset(&dst)
			if _, err := w.Write(body); err != nil {
				gzipPool.Put(w)
				return nil, err
			}
			if err := w.Close(); err != nil {
				gzipPool.Put(w)
				return nil, err
			}
			w.Reset(io.Discard)
			gzipPool.Put(w)
			return dst.Bytes(), nil
		})
		return ctx.Next()
	}
}

func acceptsGzip(header string) bool {
	var foundGzip, gzipOK, foundStar, starOK bool

	i := 0
	for i < len(header) {
		for i < len(header) && (header[i] == ',' || header[i] == ' ') {
			i++
		}
		if i >= len(header) {
			break
		}
		start := i
		for i < len(header) && header[i] != ';' && header[i] != ',' && header[i] != ' ' {
			i++
		}
		name := header[start:i]

		for i < len(header) && header[i] == ' ' {
			i++
		}

		qZero := false
		if i < len(header) && header[i] == ';' {
			i++
			qZero = isQZero(header[i:])
		}

		for i < len(header) && header[i] != ',' {
			i++
		}
		if i < len(header) {
			i++
		}

		if strings.EqualFold(name, "gzip") {
			foundGzip = true
			gzipOK = !qZero
		} else if len(name) == 1 && name[0] == '*' {
			foundStar = true
			starOK = !qZero
		}
	}

	if foundGzip {
		return gzipOK
	}
	if foundStar {
		return starOK
	}
	return false
}

func isQZero(s string) bool {
	for len(s) > 0 && s[0] == ' ' {
		s = s[1:]
	}
	if len(s) < 3 || (s[0] != 'q' && s[0] != 'Q') || s[1] != '=' {
		return false
	}
	s = s[2:]
	for len(s) > 0 && s[0] == ' ' {
		s = s[1:]
	}
	if len(s) == 0 {
		return false
	}
	if s[0] != '0' {
		return false
	}
	if len(s) == 1 {
		return true
	}
	if s[1] != '.' {
		return true
	}
	for i := 2; i < len(s); i++ {
		c := s[i]
		if c == ',' || c == ';' || c == ' ' {
			return true
		}
		if c != '0' {
			return false
		}
	}
	return true
}

// ── BasicAuth ──────────────────────────────────────────────────────────────

func BasicAuth(username, password string) fh.HandlerFunc {
	expected := "Basic " + base64Encode(username+":"+password)
	return func(ctx *fh.Ctx) error {
		auth := ctx.Get("Authorization")
		if len(auth) != len(expected) || subtle.ConstantTimeCompare([]byte(auth), []byte(expected)) != 1 {
			ctx.Set("WWW-Authenticate", `Basic realm="Restricted"`)
			return ctx.Status(401).SendString("Unauthorized")
		}
		return ctx.Next()
	}
}

// ── Timeout ────────────────────────────────────────────────────────────────

func Timeout(d time.Duration) fh.HandlerFunc {
	return func(ctx *fh.Ctx) error {
		deadline, cancel := context.WithTimeout(ctx.Context(), d)
		defer cancel()
		ctx.SetContext(deadline)
		err := ctx.Next()
		if errors.Is(deadline.Err(), context.DeadlineExceeded) && !ctx.Responded() {
			return ctx.Status(503).SendString("Request Timeout")
		}
		return err
	}
}

// ── IP Whitelist ───────────────────────────────────────────────────────────

func IPWhitelist(allowed ...string) fh.HandlerFunc {
	networks := make([]*net.IPNet, 0, len(allowed))
	ips := make([]net.IP, 0, len(allowed))

	for _, a := range allowed {
		if strings.Contains(a, "/") {
			_, n, err := net.ParseCIDR(a)
			if err == nil {
				networks = append(networks, n)
			}
		} else {
			if ip := net.ParseIP(a); ip != nil {
				ips = append(ips, ip)
			}
		}
	}

	return func(ctx *fh.Ctx) error {
		clientIP := net.ParseIP(ctx.IP())
		if clientIP == nil {
			return ctx.Status(403).SendString("Forbidden")
		}
		for _, ip := range ips {
			if ip.Equal(clientIP) {
				return ctx.Next()
			}
		}
		for _, n := range networks {
			if n.Contains(clientIP) {
				return ctx.Next()
			}
		}
		return ctx.Status(403).SendString("Forbidden")
	}
}

// ── Security Headers ───────────────────────────────────────────────────────

type SecurityConfig struct {
	ContentSecurityPolicy string
	HSTSMaxAge            int
	HSTSIncludeSubDomains bool
	FrameDeny             bool
	ContentTypeNosniff    bool
	XSSProtection         string
	ReferrerPolicy        string
	PermissionsPolicy     string
}

var defaultSecurityConfig = SecurityConfig{
	ContentSecurityPolicy: "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; form-action 'self'",
	HSTSMaxAge:            31536000,
	HSTSIncludeSubDomains: true,
	FrameDeny:             true,
	ContentTypeNosniff:    true,
	XSSProtection:         "0",
	ReferrerPolicy:        "no-referrer",
	PermissionsPolicy:     "geolocation=(), microphone=(), camera=(), payment=(), usb=(), magnetometer=(), accelerometer=(), gyroscope=(), interest-cohort=()",
}

func SecurityHeaders(config ...SecurityConfig) fh.HandlerFunc {
	cfg := defaultSecurityConfig
	if len(config) > 0 {
		cfg = config[0]
	}

	var static [][2]string
	if cfg.ContentSecurityPolicy != "" {
		static = append(static, [2]string{"Content-Security-Policy", cfg.ContentSecurityPolicy})
	}
	if cfg.FrameDeny {
		static = append(static, [2]string{"X-Frame-Options", "DENY"})
	}
	if cfg.ContentTypeNosniff {
		static = append(static, [2]string{"X-Content-Type-Options", "nosniff"})
	}
	if cfg.XSSProtection != "" {
		static = append(static, [2]string{"X-XSS-Protection", cfg.XSSProtection})
	}
	if cfg.ReferrerPolicy != "" {
		static = append(static, [2]string{"Referrer-Policy", cfg.ReferrerPolicy})
	}
	if cfg.PermissionsPolicy != "" {
		static = append(static, [2]string{"Permissions-Policy", cfg.PermissionsPolicy})
	}
	if cfg.HSTSMaxAge > 0 {
		hsts := "max-age=" + strconv.Itoa(cfg.HSTSMaxAge)
		if cfg.HSTSIncludeSubDomains {
			hsts += "; includeSubDomains"
		}
		static = append(static, [2]string{"Strict-Transport-Security", hsts})
	}

	return func(ctx *fh.Ctx) error {
		for _, h := range static {
			ctx.Set(h[0], h[1])
		}
		return ctx.Next()
	}
}

// ── Helpers ────────────────────────────────────────────────────────────────

const b64chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

func base64Encode(s string) string {
	src := []byte(s)
	dst := make([]byte, (len(src)+2)/3*4)
	n := 0
	for i := 0; i < len(src); i += 3 {
		var b0, b1, b2 byte
		b0 = src[i]
		if i+1 < len(src) {
			b1 = src[i+1]
		}
		if i+2 < len(src) {
			b2 = src[i+2]
		}
		dst[n] = b64chars[b0>>2]
		dst[n+1] = b64chars[((b0&0x3)<<4)|b1>>4]
		dst[n+2] = b64chars[((b1&0xf)<<2)|b2>>6]
		dst[n+3] = b64chars[b2&0x3f]
		n += 4
	}
	switch len(src) % 3 {
	case 1:
		dst[n-2] = '='
		dst[n-1] = '='
	case 2:
		dst[n-1] = '='
	}
	return string(dst)
}
