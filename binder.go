package fh

import (
	"errors"
	"net/url"
	"reflect"
	"strconv"
	"strings"
)

var (
	ErrInvalidBindTarget = errors.New("fh: bind target must be a non-nil pointer")
)

// Bind parses request body, query params, and headers into target struct v based on struct tags.
func Bind(c Ctx, v any) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return ErrInvalidBindTarget
	}
	elem := rv.Elem()
	if elem.Kind() != reflect.Struct {
		return ErrInvalidBindTarget
	}

	ct := strings.ToLower(c.ResponseHeader("Content-Type"))
	if ct == "" {
		if reqHeaders := c.GetReqHeaders(); len(reqHeaders) > 0 {
			if vals, ok := reqHeaders["content-type"]; ok && len(vals) > 0 {
				ct = strings.ToLower(vals[0])
			}
		}
	}

	if strings.Contains(ct, "application/json") || ct == "" {
		if len(c.Body()) > 0 {
			_ = BindJSON(c, v)
		}
	} else if strings.Contains(ct, "form") {
		_ = BindForm(c, v)
	}

	_ = BindQuery(c, v)
	_ = BindHeader(c, v)

	return nil
}

// BindJSON unmarshals request body into target struct v.
func BindJSON(c Ctx, v any) error {
	return c.BodyParser(v)
}

// BindQuery parses URL query string parameters into target struct v based on `query:"key"` tags.
func BindQuery(c Ctx, v any) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return ErrInvalidBindTarget
	}
	elem := rv.Elem()
	if elem.Kind() != reflect.Struct {
		return ErrInvalidBindTarget
	}

	typ := elem.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := field.Tag.Get("query")
		if tag == "" || tag == "-" {
			continue
		}
		val := c.Query(tag)
		if val == "" {
			continue
		}
		setBinderField(elem.Field(i), val)
	}
	return nil
}

// BindForm parses form parameters into target struct v based on `form:"key"` tags.
func BindForm(c Ctx, v any) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return ErrInvalidBindTarget
	}
	elem := rv.Elem()
	if elem.Kind() != reflect.Struct {
		return ErrInvalidBindTarget
	}

	body := string(c.Body())
	values, err := url.ParseQuery(body)
	if err != nil {
		return err
	}

	typ := elem.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := field.Tag.Get("form")
		if tag == "" || tag == "-" {
			tag = field.Tag.Get("json")
		}
		if tag == "" || tag == "-" {
			continue
		}
		if vals, ok := values[tag]; ok && len(vals) > 0 {
			setBinderField(elem.Field(i), vals[0])
		}
	}
	return nil
}

// BindHeader parses request headers into target struct v based on `header:"Header-Name"` tags.
func BindHeader(c Ctx, v any) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return ErrInvalidBindTarget
	}
	elem := rv.Elem()
	if elem.Kind() != reflect.Struct {
		return ErrInvalidBindTarget
	}

	typ := elem.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := field.Tag.Get("header")
		if tag == "" || tag == "-" {
			continue
		}
		val := c.Get(tag)
		if val == "" {
			continue
		}
		setBinderField(elem.Field(i), val)
	}
	return nil
}

func setBinderField(field reflect.Value, strVal string) {
	if !field.CanSet() {
		return
	}
	switch field.Kind() {
	case reflect.String:
		field.SetString(strVal)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if i, err := strconv.ParseInt(strVal, 10, 64); err == nil {
			field.SetInt(i)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if u, err := strconv.ParseUint(strVal, 10, 64); err == nil {
			field.SetUint(u)
		}
	case reflect.Float32, reflect.Float64:
		if f, err := strconv.ParseFloat(strVal, 64); err == nil {
			field.SetFloat(f)
		}
	case reflect.Bool:
		if b, err := strconv.ParseBool(strVal); err == nil {
			field.SetBool(b)
		}
	}
}
