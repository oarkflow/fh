package fasthttp

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/textproto"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Codec unmarshals request bodies for a content type.
//
// This interface intentionally stays tiny for fast hot-path dispatch.
// Codecs can optionally implement ContentTypeAwareCodec, EncoderCodec, or ResettableCodec.
type Codec interface {
	// ContentType returns the canonical MIME type handled by this codec,
	// for example application/json.
	ContentType() string

	// Unmarshal decodes data into v.
	Unmarshal(data []byte, v any) error
}

// ContentTypeAwareCodec is implemented by codecs that need access to media type
// parameters, for example multipart/form-data; boundary=...
type ContentTypeAwareCodec interface {
	Codec
	UnmarshalWithContentType(data []byte, contentType string, v any) error
}

// EncoderCodec is optional. It lets the same registry support response encoding.
type EncoderCodec interface {
	Codec
	Marshal(v any) ([]byte, error)
}

// ResettableCodec is optional for codecs that own reusable internal buffers.
type ResettableCodec interface {
	Codec
	Reset()
}

// FormBinder allows applications to bind form data into structs without this
// package using reflection. Implement this on DTOs when you need typed binding.
type FormBinder interface {
	BindForm(Form) error
}

// MultipartBinder allows applications to bind multipart data into structs without reflection.
type MultipartBinder interface {
	BindMultipart(*MultipartForm) error
}

// Form is the decoded representation of application/x-www-form-urlencoded.
// Values stores exact repeated-key values. Tree stores bracket-notation nesting.
type Form struct {
	Values url.Values
	Tree   map[string]any
}

// First returns the first value for key, or empty string.
func (f Form) First(key string) string {
	if len(f.Values) == 0 {
		return ""
	}
	return f.Values.Get(key)
}

// Strings returns all values for key.
func (f Form) Strings(key string) []string { return f.Values[key] }

// Int parses the first value for key.
func (f Form) Int(key string) (int, error) { return strconv.Atoi(f.First(key)) }

// Int64 parses the first value for key.
func (f Form) Int64(key string) (int64, error) { return strconv.ParseInt(f.First(key), 10, 64) }

// Uint64 parses the first value for key.
func (f Form) Uint64(key string) (uint64, error) { return strconv.ParseUint(f.First(key), 10, 64) }

// Float64 parses the first value for key.
func (f Form) Float64(key string) (float64, error) { return strconv.ParseFloat(f.First(key), 64) }

// Bool parses the first value for key.
func (f Form) Bool(key string) (bool, error) { return strconv.ParseBool(f.First(key)) }

// MultipartFile is an in-memory uploaded file.
// Body parsers should enforce request-body limits before calling codecs.
type MultipartFile struct {
	FieldName string
	FileName  string
	Header    textproto.MIMEHeader
	Size      int64
	Data      []byte
}

// MultipartForm is the decoded representation of multipart/form-data.
type MultipartForm struct {
	Values url.Values
	Files  map[string][]MultipartFile
}

// First returns the first field value for key, or empty string.
func (m *MultipartForm) First(key string) string {
	if m == nil || len(m.Values) == 0 {
		return ""
	}
	return m.Values.Get(key)
}

// CodecOptions controls defensive limits. These limits are intentionally sane
// defaults and can be changed at process startup with SetCodecOptions.
type CodecOptions struct {
	MaxFormPairs          int
	MaxFormKeyBytes       int
	MaxFormValueBytes     int
	MaxFormDepth          int
	MaxMultipartParts     int
	MaxMultipartFieldSize int64
	MaxMultipartFileSize  int64
	MaxNDJSONLineBytes    int
	MaxCSVRecordBytes     int
}

var defaultCodecOptions = CodecOptions{
	MaxFormPairs:          10_000,
	MaxFormKeyBytes:       4 << 10,
	MaxFormValueBytes:     4 << 20,
	MaxFormDepth:          32,
	MaxMultipartParts:     10_000,
	MaxMultipartFieldSize: 8 << 20,
	MaxMultipartFileSize:  64 << 20,
	MaxNDJSONLineBytes:    8 << 20,
	MaxCSVRecordBytes:     8 << 20,
}

var codecMu sync.RWMutex
var codecs map[string]Codec
var codecOrder []string // longest first
var codecOptions = defaultCodecOptions

// SetCodecOptions updates defensive codec limits. Zero values keep current/default values.
func SetCodecOptions(opt CodecOptions) {
	codecMu.Lock()
	defer codecMu.Unlock()
	if opt.MaxFormPairs > 0 {
		codecOptions.MaxFormPairs = opt.MaxFormPairs
	}
	if opt.MaxFormKeyBytes > 0 {
		codecOptions.MaxFormKeyBytes = opt.MaxFormKeyBytes
	}
	if opt.MaxFormValueBytes > 0 {
		codecOptions.MaxFormValueBytes = opt.MaxFormValueBytes
	}
	if opt.MaxFormDepth > 0 {
		codecOptions.MaxFormDepth = opt.MaxFormDepth
	}
	if opt.MaxMultipartParts > 0 {
		codecOptions.MaxMultipartParts = opt.MaxMultipartParts
	}
	if opt.MaxMultipartFieldSize > 0 {
		codecOptions.MaxMultipartFieldSize = opt.MaxMultipartFieldSize
	}
	if opt.MaxMultipartFileSize > 0 {
		codecOptions.MaxMultipartFileSize = opt.MaxMultipartFileSize
	}
	if opt.MaxNDJSONLineBytes > 0 {
		codecOptions.MaxNDJSONLineBytes = opt.MaxNDJSONLineBytes
	}
	if opt.MaxCSVRecordBytes > 0 {
		codecOptions.MaxCSVRecordBytes = opt.MaxCSVRecordBytes
	}
}

func getCodecOptions() CodecOptions {
	codecMu.RLock()
	opt := codecOptions
	codecMu.RUnlock()
	return opt
}

// RegisterCodec registers or replaces a codec. It panics only for programmer errors.
// Use RegisterCodecStrict when you prefer an error return.
func RegisterCodec(c Codec) {
	if err := RegisterCodecStrict(c); err != nil {
		panic(err)
	}
}

// RegisterCodecStrict registers or replaces a codec.
func RegisterCodecStrict(c Codec) error {
	if c == nil {
		return errors.New("codec: nil codec")
	}
	ct := normalizeContentType(c.ContentType())
	if ct == "" {
		return errors.New("codec: empty content type")
	}

	codecMu.Lock()
	defer codecMu.Unlock()
	if codecs == nil {
		codecs = make(map[string]Codec, 16)
	}
	if _, exists := codecs[ct]; !exists {
		codecOrder = append(codecOrder, ct)
	}
	codecs[ct] = c
	sort.Slice(codecOrder, func(i, j int) bool {
		if len(codecOrder[i]) == len(codecOrder[j]) {
			return codecOrder[i] < codecOrder[j]
		}
		return len(codecOrder[i]) > len(codecOrder[j])
	})
	return nil
}

// RegisterCodecAlias registers another content type for an existing codec.
func RegisterCodecAlias(contentType string, c Codec) {
	RegisterCodec(aliasCodec{ct: contentType, Codec: c})
}

type aliasCodec struct {
	ct string
	Codec
}

func (a aliasCodec) ContentType() string { return a.ct }

// DecodeBody decodes data using the registered codec for contentType.
func DecodeBody(data []byte, contentType string, v any) error {
	if v == nil {
		return errors.New("codec: nil target")
	}
	c := matchCodec(contentType)
	if c == nil {
		return fmt.Errorf("codec: unsupported content type %q", contentType)
	}
	if aware, ok := c.(ContentTypeAwareCodec); ok {
		return aware.UnmarshalWithContentType(data, contentType, v)
	}
	return c.Unmarshal(data, v)
}

// EncodeBody encodes v using a registered encoder codec.
func EncodeBody(contentType string, v any) ([]byte, error) {
	c := matchCodec(contentType)
	if c == nil {
		return nil, fmt.Errorf("codec: unsupported content type %q", contentType)
	}
	enc, ok := c.(EncoderCodec)
	if !ok {
		return nil, fmt.Errorf("codec: content type %q does not support marshal", contentType)
	}
	return enc.Marshal(v)
}

func matchCodec(contentType string) Codec {
	ct := normalizeContentType(contentType)
	if ct == "" {
		return nil
	}
	codecMu.RLock()
	defer codecMu.RUnlock()
	if codecs == nil {
		return nil
	}
	if c := codecs[ct]; c != nil {
		return c
	}
	// Structured syntax suffix support: application/problem+json => json.
	if i := strings.LastIndexByte(ct, '+'); i > 0 && i+1 < len(ct) {
		suffix := ct[i+1:]
		if c := codecs["application/"+suffix]; c != nil {
			return c
		}
	}
	// Longest prefix fallback for compatibility with existing code.
	for _, prefix := range codecOrder {
		if strings.HasPrefix(ct, prefix) {
			return codecs[prefix]
		}
	}
	return nil
}

func normalizeContentType(contentType string) string {
	contentType = strings.TrimSpace(strings.ToLower(contentType))
	if contentType == "" {
		return ""
	}
	mt, _, err := mime.ParseMediaType(contentType)
	if err == nil && mt != "" {
		return strings.ToLower(mt)
	}
	if semi := strings.IndexByte(contentType, ';'); semi >= 0 {
		contentType = strings.TrimSpace(contentType[:semi])
	}
	return contentType
}

// DecodeForm parses application/x-www-form-urlencoded or query-string data.
func DecodeForm(data []byte, v any) error { return formCodec{}.Unmarshal(data, v) }

// ── JSON ─────────────────────────────────────────────────────────────────────

type jsonCodec struct{}

func (jsonCodec) ContentType() string { return "application/json" }
func (jsonCodec) Unmarshal(data []byte, v any) error {
	if len(data) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("json: %w", err)
	}
	if dec.More() {
		return errors.New("json: multiple top-level values")
	}
	return nil
}
func (jsonCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

// ── XML ──────────────────────────────────────────────────────────────────────

type xmlCodec struct{}

func (xmlCodec) ContentType() string { return "application/xml" }
func (xmlCodec) Unmarshal(data []byte, v any) error {
	if len(data) == 0 {
		return nil
	}
	if err := xml.Unmarshal(data, v); err != nil {
		return fmt.Errorf("xml: %w", err)
	}
	return nil
}
func (xmlCodec) Marshal(v any) ([]byte, error) { return xml.Marshal(v) }

// ── Text-like codecs ─────────────────────────────────────────────────────────

type textCodec struct{ ct string }

func (c textCodec) ContentType() string { return c.ct }
func (c textCodec) Unmarshal(data []byte, v any) error {
	switch dst := v.(type) {
	case *string:
		*dst = string(data)
		return nil
	case *[]byte:
		*dst = append((*dst)[:0], data...)
		return nil
	case *any:
		*dst = string(data)
		return nil
	default:
		return fmt.Errorf("%s: unsupported target %T; use *string, *[]byte, or *any", c.ct, v)
	}
}
func (c textCodec) Marshal(v any) ([]byte, error) {
	switch x := v.(type) {
	case string:
		return []byte(x), nil
	case []byte:
		return x, nil
	case fmt.Stringer:
		return []byte(x.String()), nil
	default:
		return nil, fmt.Errorf("%s: unsupported marshal type %T", c.ct, v)
	}
}

// ── Binary codecs ────────────────────────────────────────────────────────────

type binaryCodec struct{ ct string }

func (c binaryCodec) ContentType() string { return c.ct }
func (c binaryCodec) Unmarshal(data []byte, v any) error {
	switch dst := v.(type) {
	case *[]byte:
		*dst = append((*dst)[:0], data...)
		return nil
	case *string:
		*dst = string(data)
		return nil
	case *any:
		buf := make([]byte, len(data))
		copy(buf, data)
		*dst = buf
		return nil
	default:
		return fmt.Errorf("%s: unsupported target %T; use *[]byte, *string, or *any", c.ct, v)
	}
}
func (c binaryCodec) Marshal(v any) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		return x, nil
	case string:
		return []byte(x), nil
	default:
		return nil, fmt.Errorf("%s: unsupported marshal type %T", c.ct, v)
	}
}

// ── Form ─────────────────────────────────────────────────────────────────────

type formCodec struct{}

func (formCodec) ContentType() string { return "application/x-www-form-urlencoded" }
func (formCodec) Unmarshal(data []byte, v any) error {
	form, err := parseFormBytes(data)
	if err != nil {
		return err
	}
	switch dst := v.(type) {
	case *Form:
		*dst = form
		return nil
	case *url.Values:
		*dst = cloneValues(form.Values)
		return nil
	case *map[string][]string:
		*dst = mapFromValues(form.Values)
		return nil
	case *map[string]any:
		if *dst == nil {
			*dst = make(map[string]any, len(form.Tree))
		}
		for k, vv := range form.Tree {
			(*dst)[k] = vv
		}
		return nil
	case *any:
		*dst = form.Tree
		return nil
	case FormBinder:
		return dst.BindForm(form)
	default:
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Ptr && !rv.IsNil() && rv.Elem().Kind() == reflect.Struct {
			return decodeFormToStruct(rv.Elem(), form.Tree)
		}
		return fmt.Errorf("form: unsupported target %T; use *Form, *url.Values, *map[string][]string, *map[string]any, *any, or FormBinder", v)
	}
}

// decodeFormToStruct populates a struct value from a form tree using `form:` tags.
func decodeFormToStruct(rv reflect.Value, tree map[string]any) error {
	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		if !field.IsExported() {
			continue
		}
		fv := rv.Field(i)
		if !fv.CanSet() {
			continue
		}
		name := field.Tag.Get("form")
		if name == "" {
			name = strings.ToLower(field.Name)
		}
		raw, ok := tree[name]
		if !ok {
			continue
		}
		if err := setStructField(fv, raw); err != nil {
			return fmt.Errorf("form: field %q: %w", field.Name, err)
		}
	}
	return nil
}

func setStructField(fv reflect.Value, raw any) error {
	switch fv.Kind() {
	case reflect.String:
		s, err := toString(raw)
		if err != nil {
			return err
		}
		fv.SetString(s)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		s, err := toString(raw)
		if err != nil {
			return err
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		fv.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		s, err := toString(raw)
		if err != nil {
			return err
		}
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return err
		}
		fv.SetUint(n)
	case reflect.Float32, reflect.Float64:
		s, err := toString(raw)
		if err != nil {
			return err
		}
		fl, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		fv.SetFloat(fl)
	case reflect.Bool:
		s, err := toString(raw)
		if err != nil {
			return err
		}
		b, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		fv.SetBool(b)
	case reflect.Slice:
		if fv.Type().Elem().Kind() == reflect.String {
			switch r := raw.(type) {
			case string:
				fv.Set(reflect.ValueOf([]string{r}))
			case []string:
				fv.Set(reflect.ValueOf(r))
			case []any:
				strs := make([]string, len(r))
				for i, v := range r {
					strs[i] = fmt.Sprint(v)
				}
				fv.Set(reflect.ValueOf(strs))
			default:
				return fmt.Errorf("cannot set []string from %T", raw)
			}
		}
	case reflect.Ptr:
		if fv.Type().Elem().Kind() == reflect.String {
			s, err := toString(raw)
			if err != nil {
				return err
			}
			fv.Set(reflect.ValueOf(&s))
		}
	case reflect.Struct:
		sub, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("expected map for nested struct, got %T", raw)
		}
		return decodeFormToStruct(fv, sub)
	}
	return nil
}

func toString(raw any) (string, error) {
	switch r := raw.(type) {
	case string:
		return r, nil
	case []string:
		if len(r) > 0 {
			return r[0], nil
		}
		return "", nil
	default:
		return fmt.Sprint(raw), nil
	}
}

func (formCodec) Marshal(v any) ([]byte, error) {
	switch x := v.(type) {
	case url.Values:
		return []byte(x.Encode()), nil
	case Form:
		return []byte(x.Values.Encode()), nil
	case *Form:
		if x == nil {
			return nil, errors.New("form: nil *Form")
		}
		return []byte(x.Values.Encode()), nil
	case map[string][]string:
		vals := url.Values(x)
		return []byte(vals.Encode()), nil
	case map[string]string:
		vals := make(url.Values, len(x))
		for k, v := range x {
			vals.Set(k, v)
		}
		return []byte(vals.Encode()), nil
	default:
		return nil, fmt.Errorf("form: unsupported marshal type %T", v)
	}
}

func parseFormBytes(data []byte) (Form, error) {
	opt := getCodecOptions()
	vals := make(url.Values)
	tree := make(map[string]any)
	if len(data) == 0 {
		return Form{Values: vals, Tree: tree}, nil
	}
	pairs := 0
	n := len(data)
	i := 0
	for i < n {
		if data[i] == '&' {
			i++
			continue
		}
		start := i
		for i < n && data[i] != '&' {
			i++
		}
		end := i
		if end > start {
			pairs++
			if pairs > opt.MaxFormPairs {
				return Form{}, fmt.Errorf("form: too many pairs > %d", opt.MaxFormPairs)
			}

			eq := -1
			for j := start; j < end; j++ {
				if data[j] == '=' {
					eq = j
					break
				}
			}
			var rawK, rawV []byte
			if eq >= 0 {
				rawK, rawV = data[start:eq], data[eq+1:end]
			} else {
				rawK, rawV = data[start:end], nil
			}
			if len(rawK) > opt.MaxFormKeyBytes {
				return Form{}, fmt.Errorf("form: key too large > %d bytes", opt.MaxFormKeyBytes)
			}
			if len(rawV) > opt.MaxFormValueBytes {
				return Form{}, fmt.Errorf("form: value too large > %d bytes", opt.MaxFormValueBytes)
			}
			key := urlDecode(rawK)
			val := urlDecode(rawV)
			if key == "" {
				continue
			}
			vals.Add(key, val)
			hasBracket := bytes.IndexByte(s2b(key), '[') >= 0 || bytes.IndexByte(s2b(key), ']') >= 0
			if hasBracket {
				path := parseBracketPath(key)
				if len(path) > opt.MaxFormDepth {
					return Form{}, fmt.Errorf("form: nesting too deep > %d", opt.MaxFormDepth)
				}
				insertNested(tree, path, val)
			} else {
				insertFlat(tree, key, val)
			}
		}

		if i >= n {
			break
		}
		i++
	}
	return Form{Values: vals, Tree: collapseArrays(tree).(map[string]any)}, nil
}

func parseFormPair(pair []byte, vals url.Values, tree map[string]any, opt CodecOptions) error {
	eq := bytes.IndexByte(pair, '=')
	var rawK, rawV []byte
	if eq >= 0 {
		rawK, rawV = pair[:eq], pair[eq+1:]
	} else {
		rawK, rawV = pair, nil
	}
	if len(rawK) > opt.MaxFormKeyBytes {
		return fmt.Errorf("form: key too large > %d bytes", opt.MaxFormKeyBytes)
	}
	if len(rawV) > opt.MaxFormValueBytes {
		return fmt.Errorf("form: value too large > %d bytes", opt.MaxFormValueBytes)
	}
	key := urlDecode(rawK)
	val := urlDecode(rawV)
	if key == "" {
		return nil
	}
	vals.Add(key, val)
	hasBracket := bytes.IndexByte(s2b(key), '[') >= 0 || bytes.IndexByte(s2b(key), ']') >= 0
	if hasBracket {
		path := parseBracketPath(key)
		if len(path) > opt.MaxFormDepth {
			return fmt.Errorf("form: nesting too deep > %d", opt.MaxFormDepth)
		}
		insertNested(tree, path, val)
	} else {
		insertFlat(tree, key, val)
	}
	return nil
}

func parseBracketPath(key string) []string {
	parts := make([]string, 0, 4)
	start := 0
	for i := 0; i < len(key); i++ {
		switch key[i] {
		case '[':
			if i > start {
				parts = append(parts, key[start:i])
			}
			start = i + 1
		case ']':
			parts = append(parts, key[start:i])
			start = i + 1
		}
	}
	if start < len(key) {
		parts = append(parts, key[start:])
	}
	if len(parts) == 0 {
		parts = append(parts, key)
	}
	return parts
}

func insertFlat(dest map[string]any, key, val string) {
	if existing, ok := dest[key]; ok {
		switch sl := existing.(type) {
		case []string:
			dest[key] = append(sl, val)
		case string:
			dest[key] = []string{sl, val}
		default:
			dest[key] = []any{sl, val}
		}
		return
	}
	dest[key] = val
}

func insertNested(dest map[string]any, path []string, val string) {
	if len(path) == 0 {
		return
	}
	m := dest
	for i := 0; i < len(path)-1; i++ {
		k := path[i]
		if k == "" {
			k = strconv.Itoa(nextArrayIndex(m))
		}
		next, ok := m[k]
		if !ok {
			nm := make(map[string]any)
			m[k] = nm
			m = nm
			continue
		}
		if nm, ok := next.(map[string]any); ok {
			m = nm
			continue
		}
		nm := make(map[string]any)
		m[k] = nm
		m = nm
	}
	last := path[len(path)-1]
	if last == "" {
		last = strconv.Itoa(nextArrayIndex(m))
	}
	insertFlat(m, last, val)
}

func nextArrayIndex(m map[string]any) int {
	for i := 0; ; i++ {
		if _, ok := m[strconv.Itoa(i)]; !ok {
			return i
		}
	}
}

func collapseArrays(v any) any {
	switch m := v.(type) {
	case map[string]any:
		for k, child := range m {
			m[k] = collapseArrays(child)
		}
		if arr, ok := mapAsDenseArray(m); ok {
			return arr
		}
		return m
	default:
		return v
	}
}

func mapAsDenseArray(m map[string]any) ([]any, bool) {
	if len(m) == 0 {
		return nil, false
	}
	arr := make([]any, len(m))
	for k, v := range m {
		i, err := strconv.Atoi(k)
		if err != nil || i < 0 || i >= len(m) {
			return nil, false
		}
		arr[i] = v
	}
	return arr, true
}

func cloneValues(src url.Values) url.Values {
	dst := make(url.Values, len(src))
	for k, vals := range src {
		cp := make([]string, len(vals))
		copy(cp, vals)
		dst[k] = cp
	}
	return dst
}

func mapFromValues(src url.Values) map[string][]string {
	dst := make(map[string][]string, len(src))
	for k, vals := range src {
		cp := make([]string, len(vals))
		copy(cp, vals)
		dst[k] = cp
	}
	return dst
}

// ── Multipart ────────────────────────────────────────────────────────────────

type multipartCodec struct{}

func (multipartCodec) ContentType() string { return "multipart/form-data" }
func (multipartCodec) Unmarshal(data []byte, v any) error {
	return errors.New("multipart: content-type boundary required; call DecodeBody with full Content-Type")
}
func (multipartCodec) UnmarshalWithContentType(data []byte, ct string, v any) error {
	if v == nil {
		return errors.New("multipart: nil target")
	}
	_, params, err := mime.ParseMediaType(ct)
	if err != nil {
		return fmt.Errorf("multipart: %w", err)
	}
	boundary := params["boundary"]
	if boundary == "" {
		return errors.New("multipart: missing boundary")
	}
	mf, err := parseMultipartBytes(data, boundary)
	if err != nil {
		return err
	}
	switch dst := v.(type) {
	case *MultipartForm:
		*dst = *mf
		return nil
	case *url.Values:
		*dst = cloneValues(mf.Values)
		return nil
	case *map[string][]string:
		*dst = mapFromValues(mf.Values)
		return nil
	case *map[string]any:
		if *dst == nil {
			*dst = make(map[string]any, len(mf.Values)+len(mf.Files))
		}
		for k, vals := range mf.Values {
			if len(vals) == 1 {
				(*dst)[k] = vals[0]
			} else {
				(*dst)[k] = append([]string(nil), vals...)
			}
		}
		for k, files := range mf.Files {
			(*dst)[k] = files
		}
		return nil
	case *any:
		*dst = mf
		return nil
	case MultipartBinder:
		return dst.BindMultipart(mf)
	default:
		return fmt.Errorf("multipart: unsupported target %T; use *MultipartForm, *url.Values, *map[string][]string, *map[string]any, *any, or MultipartBinder", v)
	}
}

func parseMultipartBytes(data []byte, boundary string) (*MultipartForm, error) {
	opt := getCodecOptions()
	r := multipart.NewReader(bytes.NewReader(data), boundary)
	out := &MultipartForm{Values: make(url.Values), Files: make(map[string][]MultipartFile)}
	parts := 0
	for {
		part, err := r.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("multipart: %w", err)
		}
		parts++
		if parts > opt.MaxMultipartParts {
			_ = part.Close()
			return nil, fmt.Errorf("multipart: too many parts > %d", opt.MaxMultipartParts)
		}
		name := part.FormName()
		if name == "" {
			_ = part.Close()
			continue
		}
		filename := part.FileName()
		limit := opt.MaxMultipartFieldSize
		if filename != "" {
			limit = opt.MaxMultipartFileSize
		}
		buf, err := readLimited(part, limit)
		_ = part.Close()
		if err != nil {
			return nil, fmt.Errorf("multipart %q: %w", name, err)
		}
		if filename == "" {
			out.Values.Add(name, string(buf))
			continue
		}
		out.Files[name] = append(out.Files[name], MultipartFile{
			FieldName: name,
			FileName:  filename,
			Header:    part.Header,
			Size:      int64(len(buf)),
			Data:      buf,
		})
	}
	return out, nil
}

func readLimited(r io.Reader, limit int64) ([]byte, error) {
	if limit <= 0 {
		return nil, errors.New("invalid read limit")
	}
	lr := io.LimitReader(r, limit+1)
	buf, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	if int64(len(buf)) > limit {
		return nil, fmt.Errorf("part too large > %d bytes", limit)
	}
	return buf, nil
}

// ── NDJSON ───────────────────────────────────────────────────────────────────

type ndjsonCodec struct{}

func (ndjsonCodec) ContentType() string { return "application/x-ndjson" }
func (ndjsonCodec) Unmarshal(data []byte, v any) error {
	opt := getCodecOptions()
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 0, 64*1024), opt.MaxNDJSONLineBytes)
	var raws []json.RawMessage
	line := 0
	for sc.Scan() {
		line++
		b := bytes.TrimSpace(sc.Bytes())
		if len(b) == 0 {
			continue
		}
		var tmp any
		if err := json.Unmarshal(b, &tmp); err != nil {
			return fmt.Errorf("ndjson line %d: %w", line, err)
		}
		raw := make([]byte, len(b))
		copy(raw, b)
		raws = append(raws, raw)
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("ndjson: %w", err)
	}
	switch dst := v.(type) {
	case *[]json.RawMessage:
		*dst = raws
		return nil
	case *[]map[string]any:
		items := make([]map[string]any, 0, len(raws))
		for i, raw := range raws {
			var m map[string]any
			dec := json.NewDecoder(bytes.NewReader(raw))
			dec.UseNumber()
			if err := dec.Decode(&m); err != nil {
				return fmt.Errorf("ndjson item %d: %w", i, err)
			}
			items = append(items, m)
		}
		*dst = items
		return nil
	case *any:
		*dst = raws
		return nil
	default:
		// Fallback: NDJSON becomes a JSON array and is decoded by encoding/json.
		arr := make([]byte, 0, len(data)+len(raws)+2)
		arr = append(arr, '[')
		for i, raw := range raws {
			if i > 0 {
				arr = append(arr, ',')
			}
			arr = append(arr, raw...)
		}
		arr = append(arr, ']')
		return jsonCodec{}.Unmarshal(arr, v)
	}
}

// ── CSV ──────────────────────────────────────────────────────────────────────

type csvCodec struct{}

func (csvCodec) ContentType() string { return "text/csv" }
func (csvCodec) Unmarshal(data []byte, v any) error {
	opt := getCodecOptions()
	if len(data) > opt.MaxCSVRecordBytes && opt.MaxCSVRecordBytes > 0 {
		// This is a body-level guard for accidental massive CSV decode into memory.
		// Streaming CSV should be handled by request handlers directly.
		return fmt.Errorf("csv: body too large for in-memory codec > %d bytes", opt.MaxCSVRecordBytes)
	}
	r := csv.NewReader(bytes.NewReader(data))
	r.ReuseRecord = true
	rows, err := r.ReadAll()
	if err != nil {
		return fmt.Errorf("csv: %w", err)
	}
	switch dst := v.(type) {
	case *[][]string:
		*dst = rows
		return nil
	case *[]map[string]string:
		if len(rows) == 0 {
			*dst = nil
			return nil
		}
		head := rows[0]
		items := make([]map[string]string, 0, len(rows)-1)
		for _, row := range rows[1:] {
			m := make(map[string]string, len(head))
			for i, h := range head {
				if i < len(row) {
					m[h] = row[i]
				} else {
					m[h] = ""
				}
			}
			items = append(items, m)
		}
		*dst = items
		return nil
	case *any:
		*dst = rows
		return nil
	default:
		return fmt.Errorf("csv: unsupported target %T; use *[][]string, *[]map[string]string, or *any", v)
	}
}
func (csvCodec) Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	switch rows := v.(type) {
	case [][]string:
		if err := w.WriteAll(rows); err != nil {
			return nil, err
		}
	case []map[string]string:
		if len(rows) == 0 {
			return nil, nil
		}
		head := make([]string, 0, len(rows[0]))
		for k := range rows[0] {
			head = append(head, k)
		}
		sort.Strings(head)
		if err := w.Write(head); err != nil {
			return nil, err
		}
		for _, row := range rows {
			rec := make([]string, len(head))
			for i, h := range head {
				rec[i] = row[h]
			}
			if err := w.Write(rec); err != nil {
				return nil, err
			}
		}
		w.Flush()
	default:
		return nil, fmt.Errorf("csv: unsupported marshal type %T", v)
	}
	if err := w.Error(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func init() {
	// JSON and structured +json types.
	j := jsonCodec{}
	RegisterCodec(j)
	RegisterCodecAlias("text/json", j)
	RegisterCodecAlias("application/problem+json", j)
	RegisterCodecAlias("application/ld+json", j)

	// XML and structured +xml types.
	x := xmlCodec{}
	RegisterCodec(x)
	RegisterCodecAlias("text/xml", x)
	RegisterCodecAlias("application/rss+xml", x)
	RegisterCodecAlias("application/atom+xml", x)
	RegisterCodecAlias("image/svg+xml", x)

	// Forms.
	RegisterCodec(formCodec{})
	RegisterCodec(multipartCodec{})

	// Text family.
	RegisterCodec(textCodec{ct: "text/plain"})
	RegisterCodec(textCodec{ct: "text/html"})
	RegisterCodec(textCodec{ct: "text/css"})
	RegisterCodec(textCodec{ct: "text/javascript"})
	RegisterCodec(textCodec{ct: "application/javascript"})
	RegisterCodec(textCodec{ct: "application/graphql"})
	RegisterCodec(textCodec{ct: "application/sql"})

	// Binary/file-like family.
	RegisterCodec(binaryCodec{ct: "application/octet-stream"})
	RegisterCodec(binaryCodec{ct: "application/pdf"})
	RegisterCodec(binaryCodec{ct: "application/zip"})
	RegisterCodec(binaryCodec{ct: "image/png"})
	RegisterCodec(binaryCodec{ct: "image/jpeg"})
	RegisterCodec(binaryCodec{ct: "image/gif"})
	RegisterCodec(binaryCodec{ct: "image/webp"})

	// Line and tabular payloads.
	RegisterCodec(ndjsonCodec{})
	RegisterCodec(csvCodec{})
}
