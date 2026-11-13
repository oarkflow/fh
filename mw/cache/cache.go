// Package cache provides bounded in-memory HTTP response caching.
package cache

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

type Entry struct {
	Status           int
	ContentType      string
	Body             []byte
	Headers          map[string][]string
	Created, Expires time.Time
}

type requestCachePolicy struct {
	bypass      bool
	maxAge      time.Duration
	minFresh    time.Duration
	hasMaxAge   bool
	hasMinFresh bool
}

const maxCachedBodySize = 8 << 20
const maxCachedContentTypeSize = 4096
const maxCachedHeadersSize = 16 << 10
const maxCachedHeaderCount = 128

const maxStoredEntrySize = maxCachedBodySize + maxCachedBodySize/2 + 4096
const maxCacheKeyInputSize = 64 << 10
const cacheEntryHeaderSizeV1 = 38
const cacheEntryHeaderSizeV2 = 42
const zeroTimeSentinel int64 = -1 << 63

var cacheEntryMagicV1 = [4]byte{'F', 'H', 'C', 1}
var cacheEntryMagicV2 = [4]byte{'F', 'H', 'C', 2}
var sensitiveQueryMarkers = [...]string{"authorization", "token", "secret", "api_key", "apikey", "private_key", "credential", "cookie", "session", "tenant", "account", "identity", "principal", "password", "passwd", "auth", "user_id", "userid", "username", "user_name", "user_role", "x_user", "email", "phone", "ssn", "card_number", "date_of_birth"}

type Config struct {
	TTL                 time.Duration
	MaxBodySize         int
	MaxEntries          int
	Methods             []string
	VaryHeaders         []string
	AllowRequestCookies bool
	Store               kv.Store
	KeyGenerator        func(fh.Ctx) string
	Next                func(fh.Ctx) bool
}

var DefaultConfig = Config{TTL: time.Minute, MaxBodySize: 256 << 10, MaxEntries: 128, Methods: []string{"GET", "HEAD"}}

func New(config ...Config) fh.HandlerFunc {
	cfg := DefaultConfig
	if len(config) > 0 {
		merge(&cfg, config[0])
	}
	if cfg.Store == nil {
		cfg.Store = kv.NewMemoryStore(kv.WithMaxEntries(cfg.MaxEntries))
	}
	if cfg.KeyGenerator == nil {
		cfg.KeyGenerator = func(c fh.Ctx) string {
			return string(c.RequestHeader().Host) + " " + string(c.RequestHeader().URI)
		}
	}
	methods := make(map[string]struct{}, len(cfg.Methods))
	for _, m := range cfg.Methods {
		methods[strings.ToUpper(m)] = struct{}{}
	}
	varyHeaders := make(map[string]struct{}, len(cfg.VaryHeaders)+1)
	varyNames := make([]string, 0, len(cfg.VaryHeaders)+1)
	varyNameBytes := make([][]byte, 0, len(cfg.VaryHeaders)+1)
	for _, header := range cfg.VaryHeaders {
		name := strings.ToLower(strings.TrimSpace(header))
		if name == "" {
			continue
		}
		if _, exists := varyHeaders[name]; !exists {
			varyHeaders[name] = struct{}{}
			varyNames = append(varyNames, name)
			varyNameBytes = append(varyNameBytes, []byte(name))
		}
	}
	if cfg.AllowRequestCookies {
		if _, exists := varyHeaders["cookie"]; !exists {
			varyHeaders["cookie"] = struct{}{}
			varyNames = append(varyNames, "cookie")
			varyNameBytes = append(varyNameBytes, []byte("cookie"))
		}
	}
	return func(c fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}
		policy, policyOK := parseRequestCachePolicyHeaders(c)
		if _, ok := methods[c.Method()]; !ok || !policyOK || policy.bypass || hasNonEmptyRequestHeader(c, "Authorization") || !cfg.AllowRequestCookies && hasNonEmptyRequestHeader(c, "Cookie") || requestPragmaNoCache(c) || hasConditionalRequest(c) || hasSensitiveQuery(c) || hasUnvariedIdentityHeader(c, varyHeaders) {
			return c.Next()
		}
		var keyBuilder strings.Builder
		appendKeyPart(&keyBuilder, c.Method())
		appendKeyPart(&keyBuilder, c.Protocol())
		appendKeyPart(&keyBuilder, cfg.KeyGenerator(c))
		for i, header := range varyNames {
			appendKeyPart(&keyBuilder, header)
			valueCount := 0
			c.RequestHeader().VisitAll(func(key, _ []byte) {
				if bytes.EqualFold(key, varyNameBytes[i]) {
					valueCount++
				}
			})
			fallbackValue := ""
			if valueCount == 0 {
				fallbackValue = c.Get(header)
				if fallbackValue != "" {
					valueCount = 1
				}
			}
			appendKeyPart(&keyBuilder, strconv.Itoa(valueCount))
			if valueCount > 0 && fallbackValue == "" {
				c.RequestHeader().VisitAll(func(key, value []byte) {
					if bytes.EqualFold(key, varyNameBytes[i]) {
						appendKeyPartBytes(&keyBuilder, value)
					}
				})
			} else if fallbackValue != "" {
				appendKeyPart(&keyBuilder, fallbackValue)
			}
		}
		if keyBuilder.Len() > maxCacheKeyInputSize {
			return c.Next()
		}
		digest := sha256.Sum256([]byte(keyBuilder.String()))
		key := "fh-cache-v1:" + hex.EncodeToString(digest[:])
		if entry, ok := Get(cfg.Store, key); ok && requestEntryFresh(entry, policy, time.Now()) {
			c.Set("Age", strconv.Itoa(int(time.Since(entry.Created).Seconds())))
			c.Set("X-Cache", "HIT")
			if entry.ContentType != "" {
				c.Type(entry.ContentType)
			}
			for name, values := range entry.Headers {
				for i, value := range values {
					if i == 0 {
						c.Set(name, value)
					} else {
						c.Append(name, value)
					}
				}
			}
			return c.Status(entry.Status).SendBytes(entry.Body)
		}
		c.Set("X-Cache", "MISS")
		c.AddBodyTransform(func(body []byte) ([]byte, error) {
			if c.StatusCode() != fh.StatusOK || len(body) > cfg.MaxBodySize || len(body) > maxCachedBodySize || c.HasResponseCookies() {
				return body, nil
			}
			responseHeaders := c.GetRespHeaders()
			if hasResponseHeader(responseHeaders, "Set-Cookie") {
				return body, nil
			}
			cacheControl := strings.ToLower(joinResponseHeaderValues(responseHeaders, "Cache-Control"))
			varySafe := responseVarySafe(joinResponseHeaderValues(responseHeaders, "Vary"), varyHeaders)
			expires, expiresOK := singleResponseHeader(responseHeaders, "Expires")
			age, ageOK := singleResponseHeader(responseHeaders, "Age")
			if !expiresOK || !ageOK {
				return body, nil
			}
			contentType, contentTypeOK := singleResponseHeader(responseHeaders, "Content-Type")
			if !contentTypeOK {
				return body, nil
			}
			now := time.Now()
			ttl, originAge, fresh := responseTTL(cacheControl, expires, age, now, cfg.TTL)
			if fresh && varySafe {
				headers, headersOK := cacheResponseHeaders(responseHeaders)
				if headersOK {
					Set(cfg.Store, key, Entry{Status: c.StatusCode(), ContentType: contentType, Body: body, Headers: headers, Created: now.Add(-originAge), Expires: now.Add(ttl)})
				}
			}
			return body, nil
		})
		return c.Next()
	}
}

func hasResponseHeader(headers map[string][]string, name string) bool {
	for key, values := range headers {
		if strings.EqualFold(key, name) && len(values) != 0 {
			return true
		}
	}
	return false
}

func joinResponseHeaderValues(headers map[string][]string, name string) string {
	var first string
	var joined strings.Builder
	count := 0
	for key, values := range headers {
		if !strings.EqualFold(key, name) {
			continue
		}
		for _, value := range values {
			if count == 0 {
				first = value
			} else {
				if count == 1 {
					joined.WriteString(first)
				}
				joined.WriteByte(',')
				joined.WriteString(value)
			}
			count++
		}
	}
	if count <= 1 {
		return first
	}
	return joined.String()
}

func singleResponseHeader(headers map[string][]string, name string) (string, bool) {
	var value string
	count := 0
	for key, values := range headers {
		if !strings.EqualFold(key, name) {
			continue
		}
		for _, candidate := range values {
			count++
			if count == 1 {
				value = candidate
			} else {
				return "", false
			}
		}
	}
	return value, true
}

func pragmaNoCache(value string) bool {
	for _, directive := range strings.Split(value, ",") {
		if strings.EqualFold(strings.TrimSpace(directive), "no-cache") {
			return true
		}
	}
	return false
}

func parseRequestCachePolicy(value string) (requestCachePolicy, bool) {
	var policy requestCachePolicy
	for _, directive := range strings.Split(value, ",") {
		name, raw, hasValue := strings.Cut(strings.TrimSpace(directive), "=")
		name = strings.ToLower(strings.TrimSpace(name))
		raw = strings.Trim(strings.TrimSpace(raw), "\"'")
		switch name {
		case "no-cache", "no-store":
			policy.bypass = true
		case "max-age", "min-fresh":
			if !hasValue {
				return requestCachePolicy{}, false
			}
			duration, ok := parseCacheSeconds(raw)
			if !ok {
				return requestCachePolicy{}, false
			}
			if name == "max-age" {
				if !policy.hasMaxAge || duration < policy.maxAge {
					policy.maxAge = duration
				}
				policy.hasMaxAge = true
			} else {
				if !policy.hasMinFresh || duration > policy.minFresh {
					policy.minFresh = duration
				}
				policy.hasMinFresh = true
			}
		}
	}
	return policy, true
}

func requestEntryFresh(entry Entry, policy requestCachePolicy, now time.Time) bool {
	if policy.hasMaxAge && now.Sub(entry.Created) > policy.maxAge {
		return false
	}
	return !policy.hasMinFresh || entry.Expires.Sub(now) >= policy.minFresh
}

func parseCacheSeconds(value string) (time.Duration, bool) {
	seconds, err := strconv.ParseInt(value, 10, 64)
	maxSeconds := int64((time.Duration(1<<63 - 1)) / time.Second)
	if err != nil || seconds < 0 || seconds > maxSeconds {
		return 0, false
	}
	return time.Duration(seconds) * time.Second, true
}

func parseRequestCachePolicyHeaders(c fh.Ctx) (requestCachePolicy, bool) {
	var value strings.Builder
	found := false
	c.RequestHeader().VisitAll(func(key, fieldValue []byte) {
		if !bytes.EqualFold(key, []byte("Cache-Control")) {
			return
		}
		if found {
			value.WriteByte(',')
		}
		_, _ = value.Write(fieldValue)
		found = true
	})
	if !found {
		return requestCachePolicy{}, true
	}
	return parseRequestCachePolicy(value.String())
}

func hasNonEmptyRequestHeader(c fh.Ctx, name string) bool {
	found := false
	c.RequestHeader().VisitAll(func(key, value []byte) {
		if len(value) != 0 && bytes.EqualFold(key, []byte(name)) {
			found = true
		}
	})
	return found
}

func requestPragmaNoCache(c fh.Ctx) bool {
	found := false
	c.RequestHeader().VisitAll(func(key, value []byte) {
		if bytes.EqualFold(key, []byte("Pragma")) && pragmaNoCache(string(value)) {
			found = true
		}
	})
	return found
}

func hasConditionalRequest(c fh.Ctx) bool {
	found := false
	c.RequestHeader().VisitAll(func(key, value []byte) {
		if len(value) == 0 {
			return
		}
		for _, name := range [...]string{"Range", "If-Range", "If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since"} {
			if bytes.EqualFold(key, []byte(name)) {
				found = true
				return
			}
		}
	})
	return found
}

func appendKeyPart(b *strings.Builder, value string) {
	b.WriteString(strconv.Itoa(len(value)))
	b.WriteByte(':')
	b.WriteString(value)
}

func appendKeyPartBytes(b *strings.Builder, value []byte) {
	b.WriteString(strconv.Itoa(len(value)))
	b.WriteByte(':')
	_, _ = b.Write(value)
}

func hasSensitiveQuery(c fh.Ctx) bool {
	query := c.RequestHeader().QueryString
	if len(query) == 0 {
		return false
	}
	for len(query) > 0 {
		segment := query
		if amp := bytesIndexByte(segment, '&'); amp >= 0 {
			segment, query = segment[:amp], query[amp+1:]
		} else {
			query = nil
		}
		if bytesIndexByte(segment, ';') >= 0 {
			return true
		}
		key := segment
		if equal := bytesIndexByte(key, '='); equal >= 0 {
			key = key[:equal]
		}
		if bytesIndexByte(key, '%') >= 0 || bytesIndexByte(key, '+') >= 0 {
			decoded, err := url.QueryUnescape(string(key))
			if err != nil || sensitiveName(decoded) {
				return true
			}
		} else if sensitiveName(key) {
			return true
		}
	}
	return false
}

func hasUnvariedIdentityHeader(c fh.Ctx, varied map[string]struct{}) bool {
	found := false
	c.RequestHeader().VisitAll(func(key, _ []byte) {
		if !sensitiveName(key) {
			return
		}
		name := strings.ToLower(string(key))
		if _, ok := varied[name]; !ok {
			found = true
		}
	})
	return found
}

func sensitiveName[T ~string | ~[]byte](name T) bool {
	for _, marker := range sensitiveQueryMarkers {
		if len(name) < len(marker) {
			continue
		}
		for i := 0; i <= len(name)-len(marker); i++ {
			matched := true
			for j := range marker {
				b := byte(name[i+j])
				if b >= 'A' && b <= 'Z' {
					b += 'a' - 'A'
				}
				if b == '-' || b == '.' || b == ' ' {
					b = '_'
				}
				if b != marker[j] {
					matched = false
					break
				}
			}
			if matched {
				return true
			}
		}
	}
	return sensitiveNameEquals(name, "user") || sensitiveNameEquals(name, "dob") || sensitiveNameEquals(name, "key") || sensitiveNameSuffix(name, "_key")
}

func sensitiveNameEquals[T ~string | ~[]byte](name T, want string) bool {
	if len(name) != len(want) {
		return false
	}
	for i := range want {
		b := byte(name[i])
		if b >= 'A' && b <= 'Z' {
			b += 'a' - 'A'
		}
		if b != want[i] {
			return false
		}
	}
	return true
}

func sensitiveNameSuffix[T ~string | ~[]byte](name T, suffix string) bool {
	if len(name) < len(suffix) {
		return false
	}
	for i := range suffix {
		b := byte(name[len(name)-len(suffix)+i])
		if b >= 'A' && b <= 'Z' {
			b += 'a' - 'A'
		}
		if b == '-' || b == '.' || b == ' ' {
			b = '_'
		}
		if b != suffix[i] {
			return false
		}
	}
	return true
}

func bytesIndexByte(b []byte, c byte) int {
	for i, value := range b {
		if value == c {
			return i
		}
	}
	return -1
}

func responseVarySafe(vary string, configured map[string]struct{}) bool {
	for _, name := range strings.Split(vary, ",") {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" {
			continue
		}
		if name == "*" {
			return false
		}
		if _, ok := configured[name]; !ok {
			return false
		}
	}
	return true
}

func cacheResponseHeaders(input map[string][]string) (map[string][]string, bool) {
	if len(input) == 0 {
		return nil, true
	}
	if len(input) > maxCachedHeaderCount {
		return nil, false
	}
	excluded := map[string]struct{}{
		"age": {}, "connection": {}, "content-length": {}, "content-type": {}, "date": {},
		"keep-alive": {}, "proxy-connection": {}, "proxy-authenticate": {}, "proxy-authorization": {},
		"set-cookie": {}, "te": {}, "trailer": {}, "transfer-encoding": {}, "upgrade": {}, "x-cache": {},
	}
	for key, values := range input {
		if !strings.EqualFold(key, "Connection") {
			continue
		}
		for _, value := range values {
			for _, token := range strings.Split(value, ",") {
				name := strings.ToLower(strings.TrimSpace(token))
				if name != "" {
					if !validCacheHeaderName(name) {
						return nil, false
					}
					excluded[name] = struct{}{}
				}
			}
		}
	}
	result := make(map[string][]string, len(input))
	used, count := 0, 0
	for key, values := range input {
		name := strings.ToLower(strings.TrimSpace(key))
		if _, skip := excluded[name]; skip {
			continue
		}
		if !validCacheHeaderName(name) || len(values) == 0 {
			return nil, false
		}
		copyValues := make([]string, len(values))
		for i, value := range values {
			if !validCacheHeaderValue(value) {
				return nil, false
			}
			used += len(name) + len(value)
			count++
			if used > maxCachedHeadersSize || count > maxCachedHeaderCount {
				return nil, false
			}
			copyValues[i] = value
		}
		result[name] = copyValues
	}
	if len(result) == 0 {
		return nil, true
	}
	return result, true
}

func validCacheHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		b := name[i]
		if (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') {
			continue
		}
		switch b {
		case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		default:
			return false
		}
	}
	return true
}

func validCacheHeaderValue(value string) bool {
	for i := 0; i < len(value); i++ {
		b := value[i]
		if (b < 0x20 && b != '\t') || b == 0x7f {
			return false
		}
	}
	return true
}

func responseTTL(cacheControl, expires, age string, now time.Time, configured time.Duration) (time.Duration, time.Duration, bool) {
	var maxAge, sharedMaxAge *time.Duration
	for _, directive := range strings.Split(cacheControl, ",") {
		name, value, hasValue := strings.Cut(strings.TrimSpace(directive), "=")
		name = strings.ToLower(strings.TrimSpace(name))
		value = strings.Trim(strings.TrimSpace(value), "\"'")
		switch name {
		case "no-store", "no-cache", "private":
			return 0, 0, false
		case "max-age", "s-maxage":
			if !hasValue {
				return 0, 0, false
			}
			seconds, err := strconv.ParseInt(value, 10, 64)
			maxSeconds := int64((time.Duration(1<<63 - 1)) / time.Second)
			if err != nil || seconds < 0 || seconds > maxSeconds {
				return 0, 0, false
			}
			duration := time.Duration(seconds) * time.Second
			if name == "s-maxage" {
				if sharedMaxAge == nil || duration < *sharedMaxAge {
					sharedMaxAge = &duration
				}
			} else {
				if maxAge == nil || duration < *maxAge {
					maxAge = &duration
				}
			}
		}
	}

	lifetime := configured
	if sharedMaxAge != nil {
		lifetime = min(lifetime, *sharedMaxAge)
	} else if maxAge != nil {
		lifetime = min(lifetime, *maxAge)
	}
	if lifetime <= 0 {
		return 0, 0, false
	}
	var ageDuration time.Duration
	if strings.TrimSpace(age) != "" {
		seconds, err := strconv.ParseInt(strings.TrimSpace(age), 10, 64)
		maxSeconds := int64((time.Duration(1<<63 - 1)) / time.Second)
		if err != nil || seconds < 0 || seconds > maxSeconds {
			return 0, 0, false
		}
		ageDuration = time.Duration(seconds) * time.Second
		if ageDuration >= lifetime {
			return 0, 0, false
		}
		lifetime -= ageDuration
	}
	if value := strings.TrimSpace(expires); value != "" {
		expiresAt, err := http.ParseTime(value)
		if err != nil {
			return 0, 0, false
		}
		expiresIn := expiresAt.Sub(now)
		if expiresIn <= 0 {
			return 0, 0, false
		}
		lifetime = min(lifetime, expiresIn)
	}
	return lifetime, ageDuration, lifetime > 0
}

func merge(dst *Config, src Config) {
	if src.TTL > 0 {
		dst.TTL = src.TTL
	}
	if src.MaxBodySize > 0 {
		dst.MaxBodySize = src.MaxBodySize
	}
	if src.MaxEntries > 0 {
		dst.MaxEntries = src.MaxEntries
	}
	if src.Methods != nil {
		dst.Methods = src.Methods
	}
	if src.VaryHeaders != nil {
		dst.VaryHeaders = src.VaryHeaders
	}
	if src.Store != nil {
		dst.Store = src.Store
	}
	if src.KeyGenerator != nil {
		dst.KeyGenerator = src.KeyGenerator
	}
	dst.Next = src.Next
	dst.AllowRequestCookies = src.AllowRequestCookies
}

func Get(store kv.Store, key string) (Entry, bool) {
	if store == nil {
		return Entry{}, false
	}
	data, ok, err := store.Get(key)
	if err != nil || !ok {
		return Entry{}, false
	}
	if len(data) > maxStoredEntrySize {
		_ = store.Delete(key)
		return Entry{}, false
	}
	var entry Entry
	if len(data) >= 4 && data[0] == 'F' && data[1] == 'H' && data[2] == 'C' {
		var valid bool
		switch data[3] {
		case cacheEntryMagicV1[3]:
			entry, valid = decodeEntryV1(data)
		case 2:
			entry, valid = decodeEntryV2(data)
		default:
			valid = false
		}
		if !valid {
			_ = store.Delete(key)
			return Entry{}, false
		}
	} else if json.Unmarshal(data, &entry) != nil {
		_ = store.Delete(key)
		return Entry{}, false
	}
	now := time.Now()
	if entry.Status < 100 || entry.Status > 599 || len(entry.Body) > maxCachedBodySize || len(entry.ContentType) > maxCachedContentTypeSize || entry.Created.IsZero() || entry.Created.After(now) || entry.Expires.IsZero() || !entry.Expires.After(entry.Created) || !now.Before(entry.Expires) {
		_ = store.Delete(key)
		return Entry{}, false
	}
	headers, headersOK := cacheResponseHeaders(entry.Headers)
	if !headersOK {
		_ = store.Delete(key)
		return Entry{}, false
	}
	entry.Headers = headers
	return entry, true
}

func decodeEntryV1(data []byte) (Entry, bool) {
	if len(data) < cacheEntryHeaderSizeV1 {
		return Entry{}, false
	}
	contentTypeLen := uint64(binary.BigEndian.Uint32(data[30:34]))
	bodyLen := uint64(binary.BigEndian.Uint32(data[34:38]))
	if contentTypeLen > maxCachedContentTypeSize || bodyLen > maxCachedBodySize || uint64(cacheEntryHeaderSizeV1)+contentTypeLen+bodyLen != uint64(len(data)) {
		return Entry{}, false
	}
	return Entry{
		Status:      int(binary.BigEndian.Uint16(data[4:6])),
		ContentType: string(data[cacheEntryHeaderSizeV1 : cacheEntryHeaderSizeV1+int(contentTypeLen)]),
		Body:        data[cacheEntryHeaderSizeV1+int(contentTypeLen):],
		Created:     decodeCacheTime(int64(binary.BigEndian.Uint64(data[6:14])), binary.BigEndian.Uint32(data[14:18])),
		Expires:     decodeCacheTime(int64(binary.BigEndian.Uint64(data[18:26])), binary.BigEndian.Uint32(data[26:30])),
	}, true
}

func decodeEntryV2(data []byte) (Entry, bool) {
	if len(data) < cacheEntryHeaderSizeV2 {
		return Entry{}, false
	}
	contentTypeLen := uint64(binary.BigEndian.Uint32(data[30:34]))
	bodyLen := uint64(binary.BigEndian.Uint32(data[34:38]))
	headersLen := uint64(binary.BigEndian.Uint32(data[38:42]))
	if contentTypeLen > maxCachedContentTypeSize || bodyLen > maxCachedBodySize || headersLen > maxCachedHeadersSize || uint64(cacheEntryHeaderSizeV2)+contentTypeLen+bodyLen+headersLen != uint64(len(data)) {
		return Entry{}, false
	}
	contentTypeEnd := cacheEntryHeaderSizeV2 + int(contentTypeLen)
	bodyEnd := contentTypeEnd + int(bodyLen)
	entry := Entry{
		Status:      int(binary.BigEndian.Uint16(data[4:6])),
		ContentType: string(data[cacheEntryHeaderSizeV2:contentTypeEnd]),
		Body:        data[contentTypeEnd:bodyEnd],
		Created:     decodeCacheTime(int64(binary.BigEndian.Uint64(data[6:14])), binary.BigEndian.Uint32(data[14:18])),
		Expires:     decodeCacheTime(int64(binary.BigEndian.Uint64(data[18:26])), binary.BigEndian.Uint32(data[26:30])),
	}
	if headersLen > 0 && json.Unmarshal(data[bodyEnd:], &entry.Headers) != nil {
		return Entry{}, false
	}
	return entry, true
}

func Set(store kv.Store, key string, e Entry) {
	if store == nil || len(e.Body) > maxCachedBodySize || len(e.ContentType) > maxCachedContentTypeSize || e.Status < 100 || e.Status > 599 || e.Created.IsZero() || e.Expires.IsZero() || !e.Expires.After(e.Created) {
		return
	}
	headers, ok := cacheResponseHeaders(e.Headers)
	if !ok {
		return
	}
	headerData, err := json.Marshal(headers)
	if err != nil || len(headerData) > maxCachedHeadersSize {
		return
	}
	data := make([]byte, cacheEntryHeaderSizeV2+len(e.ContentType)+len(e.Body)+len(headerData))
	copy(data[:4], cacheEntryMagicV2[:])
	binary.BigEndian.PutUint16(data[4:6], uint16(e.Status))
	createdSec, createdNS := encodeCacheTime(e.Created)
	expiresSec, expiresNS := encodeCacheTime(e.Expires)
	binary.BigEndian.PutUint64(data[6:14], uint64(createdSec))
	binary.BigEndian.PutUint32(data[14:18], createdNS)
	binary.BigEndian.PutUint64(data[18:26], uint64(expiresSec))
	binary.BigEndian.PutUint32(data[26:30], expiresNS)
	binary.BigEndian.PutUint32(data[30:34], uint32(len(e.ContentType)))
	binary.BigEndian.PutUint32(data[34:38], uint32(len(e.Body)))
	binary.BigEndian.PutUint32(data[38:42], uint32(len(headerData)))
	copy(data[cacheEntryHeaderSizeV2:], e.ContentType)
	copy(data[cacheEntryHeaderSizeV2+len(e.ContentType):], e.Body)
	copy(data[cacheEntryHeaderSizeV2+len(e.ContentType)+len(e.Body):], headerData)
	ttl := time.Until(e.Expires)
	if ttl <= 0 {
		ttl = time.Nanosecond
	}
	_ = store.Set(key, data, ttl)
}

func encodeCacheTime(t time.Time) (int64, uint32) {
	if t.IsZero() {
		return zeroTimeSentinel, 0
	}
	return t.Unix(), uint32(t.Nanosecond())
}

func decodeCacheTime(sec int64, ns uint32) time.Time {
	if sec == zeroTimeSentinel {
		return time.Time{}
	}
	return time.Unix(sec, int64(ns))
}

func Delete(store kv.Store, key string) {
	if store == nil {
		return
	}
	_ = store.Delete(key)
}
