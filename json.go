package fh

import (
	"reflect"
	"strconv"
	"strings"
	"sync"
)

type JSONValue struct{ v any }

func (f JSONValue) AppendJSON(dst []byte) ([]byte, error) {
	out, _, err := appendJSONValueRef(dst, reflect.ValueOf(f.v))
	return out, err
}

func supportsJSON(v any) bool {
	if v == nil {
		return false
	}
	return JSONTypeSupported(reflect.TypeOf(v), make(map[reflect.Type]bool, 4))
}

func JSONTypeSupported(t reflect.Type, seen map[reflect.Type]bool) bool {
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
			if sf.PkgPath != "" {
				continue
			}
			if tag := sf.Tag.Get("json"); tag == "-" {
				continue
			}
			if !JSONTypeSupported(sf.Type, seen) {
				return false
			}
		}
		return true
	}
	return false
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
		return appendJSONStruct(dst, v), true, nil
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
		return strconv.AppendFloat(dst, v.Float(), 'g', -1, 32), true, nil
	case reflect.Float64:
		return strconv.AppendFloat(dst, v.Float(), 'g', -1, 64), true, nil
	}
	return dst, false, nil
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

func appendJSONStruct(dst []byte, v reflect.Value) []byte {
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
		dst, _, _ = appendJSONValueRef(dst, fv)
	}
	dst = append(dst, '}')
	return dst
}

func isJSONEmpty(v reflect.Value) bool {
	for v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return true
		}
		v = v.Elem()
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

func mapPointer(m map[string]any) uintptr {
	if m == nil {
		return 0
	}
	return reflect.ValueOf(m).Pointer()
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
	return false
}

func jsonLooksObjectOrArray(b []byte) bool {
	for len(b) > 0 {
		switch b[0] {
		case ' ', '\n', '\r', '\t':
			b = b[1:]
		case '{', '[':
			return true
		default:
			return false
		}
	}
	return false
}
