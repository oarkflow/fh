package fh

import (
	"encoding"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

var errUnsupportedJSONFloat = errors.New("json: unsupported non-finite floating-point value")

var (
	jsonMarshalerType = reflect.TypeOf((*JSONMarshaler)(nil)).Elem()
	textMarshalerType = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
)

type JSONValue struct{ v any }

var jsonSupportedTypeCache sync.Map // map[reflect.Type]bool

func (f JSONValue) AppendJSON(dst []byte) ([]byte, error) {
	out, _, err := appendJSONValueRef(dst, reflect.ValueOf(f.v))
	return out, err
}

func supportsJSON(v any) bool {
	if v == nil {
		return false
	}
	t := reflect.TypeOf(v)
	if supported, ok := jsonSupportedTypeCache.Load(t); ok {
		return supported.(bool)
	}
	supported := JSONTypeSupported(t, make(map[reflect.Type]bool, 4))
	actual, _ := jsonSupportedTypeCache.LoadOrStore(t, supported)
	return actual.(bool)
}

func JSONTypeSupported(t reflect.Type, seen map[reflect.Type]bool) bool {
	if t == nil || hasJSONMarshaler(t) {
		return false
	}
	if seen == nil {
		seen = make(map[reflect.Type]bool)
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.String, reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64:
		return true
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return false
		}
		return JSONTypeSupported(t.Elem(), seen)
	case reflect.Struct:
		if seen[t] {
			return false
		}
		seen[t] = true
		for i := 0; i < t.NumField(); i++ {
			sf := t.Field(i)
			if sf.Anonymous {
				return false
			}
			if sf.PkgPath != "" {
				continue
			}
			if tag := sf.Tag.Get("json"); tag != "" {
				if tag == "-" {
					continue
				}
				if !fastJSONTagSupported(tag) {
					return false
				}
			}
			if !JSONTypeSupported(sf.Type, seen) {
				return false
			}
		}
		return true
	}
	return false
}

func hasJSONMarshaler(t reflect.Type) bool {
	for t != nil {
		if t.Implements(jsonMarshalerType) || t.Implements(textMarshalerType) {
			return true
		}
		if t.Kind() != reflect.Pointer && (reflect.PointerTo(t).Implements(jsonMarshalerType) || reflect.PointerTo(t).Implements(textMarshalerType)) {
			return true
		}
		if t.Kind() != reflect.Pointer {
			break
		}
		t = t.Elem()
	}
	return false
}

func fastJSONTagSupported(tag string) bool {
	parts := strings.Split(tag, ",")
	for _, option := range parts[1:] {
		if option != "omitempty" {
			return false
		}
	}
	for i := 0; i < len(parts[0]); i++ {
		b := parts[0][i]
		if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' || b == '-' || b == '.') {
			return false
		}
	}
	return true
}

func appendJSONValueRef(dst []byte, v reflect.Value) ([]byte, bool, error) {
	if !v.IsValid() {
		return append(dst, "null"...), true, nil
	}
	for v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return append(dst, "null"...), true, nil
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Struct:
		return appendJSONStruct(dst, v)
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			return append(dst, "null"...), true, nil
		}
		dst = append(dst, '[')
		for i := 0; i < v.Len(); i++ {
			if i > 0 {
				dst = append(dst, ',')
			}
			var ok bool
			var err error
			dst, ok, err = appendJSONValueRef(dst, v.Index(i))
			if err != nil {
				return dst, false, err
			}
			if !ok {
				return dst, false, nil
			}
		}
		dst = append(dst, ']')
		return dst, true, nil
	case reflect.String:
		return appendJSONString(dst, v.String()), true, nil
	case reflect.Bool:
		if v.Bool() {
			return append(dst, "true"...), true, nil
		}
		return append(dst, "false"...), true, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.AppendInt(dst, v.Int(), 10), true, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.AppendUint(dst, v.Uint(), 10), true, nil
	case reflect.Float32:
		out, err := appendJSONFloat(dst, v.Float(), 32)
		return out, err == nil, err
	case reflect.Float64:
		out, err := appendJSONFloat(dst, v.Float(), 64)
		return out, err == nil, err
	}
	return dst, false, nil
}

func appendJSONFloat(dst []byte, value float64, bitSize int) ([]byte, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return dst, errUnsupportedJSONFloat
	}
	return strconv.AppendFloat(dst, value, 'g', -1, bitSize), nil
}

type JSONField struct {
	name      string
	index     []int
	omitEmpty bool
}

var JSONTypeCache sync.Map // map[reflect.Type][]JSONField

func cachedJSONFields(t reflect.Type) []JSONField {
	if v, ok := JSONTypeCache.Load(t); ok {
		return v.([]JSONField)
	}
	fields := make([]JSONField, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.PkgPath != "" { // unexported
			continue
		}
		name := sf.Name
		omit := false
		if tag := sf.Tag.Get("json"); tag != "" {
			if tag == "-" {
				continue
			}
			parts := strings.Split(tag, ",")
			if parts[0] != "" {
				name = parts[0]
			}
			for _, p := range parts[1:] {
				if p == "omitempty" {
					omit = true
					break
				}
			}
		}
		idx := make([]int, len(sf.Index))
		copy(idx, sf.Index)
		fields = append(fields, JSONField{name: name, index: idx, omitEmpty: omit})
	}
	actual, _ := JSONTypeCache.LoadOrStore(t, fields)
	return actual.([]JSONField)
}

func appendJSONStruct(dst []byte, v reflect.Value) ([]byte, bool, error) {
	t := v.Type()
	fields := cachedJSONFields(t)
	dst = append(dst, '{')
	wrote := false
	for i := range fields {
		f := fields[i]
		fv := v.FieldByIndex(f.index)
		if f.omitEmpty && isJSONEmpty(fv) {
			continue
		}
		if wrote {
			dst = append(dst, ',')
		}
		wrote = true
		dst = appendJSONString(dst, f.name)
		dst = append(dst, ':')
		var ok bool
		var err error
		dst, ok, err = appendJSONValueRef(dst, fv)
		if err != nil || !ok {
			return dst, ok, err
		}
	}
	dst = append(dst, '}')
	return dst, true, nil
}

func isJSONEmpty(v reflect.Value) bool {
	if v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
		return v.IsNil()
	}
	switch v.Kind() {
	case reflect.String, reflect.Array, reflect.Slice, reflect.Map:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	}
	return false
}

func isJSONContentTypeBytes(ct []byte) bool {
	if len(ct) == 0 {
		return true
	}
	// Fast common case: application/json or application/json; charset=utf-8.
	if len(ct) >= len("application/json") {
		base := ct[:len("application/json")]
		if bytesEqualFold(base, []byte("application/json")) {
			return len(ct) == len("application/json") || ct[len("application/json")] == ';'
		}
	}
	s := strings.ToLower(b2s(ct))
	if idx := strings.IndexByte(s, ';'); idx >= 0 {
		s = strings.TrimSpace(s[:idx])
	}
	return strings.HasSuffix(s, "+json")
}
