package fh

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ValidationErrors is a collection of field-level validation errors.
type ValidationErrors []FieldError

func (e ValidationErrors) Error() string {
	return fmt.Sprintf("validation failed: %d errors", len(e))
}

func (e ValidationErrors) Unwrap() error {
	if len(e) == 0 {
		return nil
	}
	return e
}

// Has reports whether any field error matches the given field name.
func (e ValidationErrors) Has(field string) bool {
	for _, fe := range e {
		if fe.Field == field {
			return true
		}
	}
	return false
}

// Get returns all field errors for the given field name.
func (e ValidationErrors) Get(field string) []FieldError {
	var out []FieldError
	for _, fe := range e {
		if fe.Field == field {
			out = append(out, fe)
		}
	}
	return out
}

// First returns the first field error, or nil.
func (e ValidationErrors) First() *FieldError {
	if len(e) == 0 {
		return nil
	}
	return &e[0]
}

// MarshalJSON serializes as an array of field error objects.
func (e ValidationErrors) MarshalJSON() ([]byte, error) {
	if len(e) == 0 {
		return []byte("[]"), nil
	}
	return json.Marshal([]FieldError(e))
}

// ValidationRule defines a single validation rule for a struct field.
type ValidationRule struct {
	// Tag is the struct tag name (e.g., "validate").
	Tag string
	// Param is the optional parameter (e.g., "min=3" → Param="3").
	Param string
}

// ValidationErrorBuilder accumulates field errors during validation.
type ValidationErrorBuilder struct {
	mu     sync.Mutex
	errors ValidationErrors
}

// NewValidationErrorBuilder creates a new builder.
func NewValidationErrorBuilder() *ValidationErrorBuilder {
	return &ValidationErrorBuilder{}
}

// Add appends a field error.
func (b *ValidationErrorBuilder) Add(field, code, message string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.errors = append(b.errors, FieldError{Field: field, Code: code, Message: message})
}

// Addf appends a field error with formatted message.
func (b *ValidationErrorBuilder) Addf(field, code, format string, args ...any) {
	b.Add(field, code, fmt.Sprintf(format, args...))
}

// HasErrors reports whether any errors were collected.
func (b *ValidationErrorBuilder) HasErrors() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.errors) > 0
}

// Error returns a *ValidationError if errors were collected, nil otherwise.
func (b *ValidationErrorBuilder) Error() *ValidationError {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.errors) == 0 {
		return nil
	}
	errs := make(ValidationErrors, len(b.errors))
	copy(errs, b.errors)
	return &ValidationError{Fields: errs}
}

// ValidationErrors returns the raw field errors.
func (b *ValidationErrorBuilder) ValidationErrors() ValidationErrors {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(ValidationErrors, len(b.errors))
	copy(out, b.errors)
	return out
}

// Clear resets the builder.
func (b *ValidationErrorBuilder) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.errors = b.errors[:0]
}

// ── Built-in validation rules ────────────────────────────────────────────────

// ruleFunc is the signature for built-in validation rule handlers.
// It receives the field name, reflect.Value, and the rule parameter.
// It returns an error message if validation fails, empty string if OK.
type ruleFunc func(fieldName string, v reflect.Value, param string) string

var (
	ruleMu    sync.RWMutex
	ruleFuncs = map[string]ruleFunc{
		"required":  ruleRequired,
		"min":       ruleMin,
		"max":       ruleMax,
		"len":       ruleLen,
		"eq":        ruleEq,
		"neq":       ruleNeq,
		"gt":        ruleGt,
		"gte":       ruleGte,
		"lt":        ruleLt,
		"lte":       ruleLte,
		"oneof":     ruleOneof,
		"email":     ruleEmail,
		"url":       ruleURL,
		"uri":       ruleURI,
		"uuid":      ruleUUID,
		"ip":        ruleIP,
		"ipv4":      ruleIPv4,
		"ipv6":      ruleIPv6,
		"cidr":      ruleCIDR,
		"mac":       ruleMAC,
		"alpha":     ruleAlpha,
		"alphanum":  ruleAlphanum,
		"numeric":   ruleNumeric,
		"boolean":   ruleBoolean,
		"json":      ruleJSON,
		"datetime":  ruleDatetime,
		"eqfield":   ruleEqField,
		"neqfield":  ruleNeqField,
		"gtfield":   ruleGtField,
		"gtefield":  ruleGteField,
		"ltfield":   ruleLtField,
		"ltefield":  ruleLteField,
		"contains":  ruleContains,
		"excludes":  ruleExcludes,
		"startswith": ruleStartswith,
		"endswith":  ruleEndswith,
		"prefix":    rulePrefix,
		"suffix":    ruleSuffix,
		"lenmin":    ruleLenMin,
		"lenmax":    ruleLenMax,
	}
)

// RegisterRule registers a custom validation rule. It is safe for concurrent use.
func RegisterRule(name string, fn ruleFunc) {
	ruleMu.Lock()
	defer ruleMu.Unlock()
	ruleFuncs[name] = fn
}

// getRule returns the registered rule function.
func getRule(name string) (ruleFunc, bool) {
	ruleMu.RLock()
	defer ruleMu.RUnlock()
	fn, ok := ruleFuncs[name]
	return fn, ok
}

// ── Tag parser ───────────────────────────────────────────────────────────────

// parseValidateTag parses a validate struct tag into a list of rules.
// Format: "required,min=1,max=100,email,oneof=active inactive"
func parseValidateTag(tag string) []ValidationRule {
	if tag == "" {
		return nil
	}
	var rules []ValidationRule
	for tag != "" {
		// Skip whitespace
		tag = strings.TrimLeft(tag, " ")
		if tag == "" {
			break
		}
		// Find next comma or end
		idx := strings.IndexByte(tag, ',')
		var part string
		if idx < 0 {
			part = tag
			tag = ""
		} else {
			part = tag[:idx]
			tag = tag[idx+1:]
		}
		// Split name=param
		eqIdx := strings.IndexByte(part, '=')
		if eqIdx < 0 {
			rules = append(rules, ValidationRule{Tag: part})
		} else {
			rules = append(rules, ValidationRule{Tag: part[:eqIdx], Param: part[eqIdx+1:]})
		}
	}
	return rules
}

// ── Core validation engine ──────────────────────────────────────────────────

var validateTagNames = []string{"validate", "valid", "v"}

// ValidateStruct validates a struct using its "validate" struct tags.
// It returns a *ValidationError with field-level details, or nil if valid.
func ValidateStruct(s any) error {
	if s == nil {
		return nil
	}
	rv := reflect.ValueOf(s)
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil
	}

	var builder ValidationErrorBuilder
	validateStructFields(rv, "", &builder)
	validateCrossFields(rv, "", &builder)

	if builder.HasErrors() {
		return builder.Error()
	}
	return nil
}

// ValidateStructWith runs validation and calls a custom Validator if implemented.
func ValidateStructWith(s any) error {
	if s == nil {
		return nil
	}
	if err := ValidateStruct(s); err != nil {
		return err
	}
	if v, ok := s.(Validator); ok {
		return v.Validate()
	}
	return nil
}

func validateStructFields(rv reflect.Value, prefix string, builder *ValidationErrorBuilder) {
	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		fieldVal := rv.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Skip fields with "-" tag
		fieldName := field.Name
		jsonTag := field.Tag.Get("json")
		if jsonTag == "-" {
			continue
		}
		if jsonTag != "" {
			if idx := strings.IndexByte(jsonTag, ','); idx >= 0 {
				jsonTag = jsonTag[:idx]
			}
			if jsonTag != "" {
				fieldName = jsonTag
			}
		}

		fullName := fieldName
		if prefix != "" {
			fullName = prefix + "." + fieldName
		}

		// Recurse into nested structs
		if fieldVal.Kind() == reflect.Ptr {
			if fieldVal.IsNil() {
				continue
			}
			fieldVal = fieldVal.Elem()
		}
		if fieldVal.Kind() == reflect.Struct {
			// Skip time.Time and other known types
			if fieldVal.Type() != reflect.TypeOf(time.Time{}) {
				validateStructFields(fieldVal, fullName, builder)
				continue
			}
		}

		// Get validation tag
		var tag string
		for _, tagName := range validateTagNames {
			if t := field.Tag.Get(tagName); t != "" {
				tag = t
				break
			}
		}
		if tag == "" {
			continue
		}

		rules := parseValidateTag(tag)
		for _, rule := range rules {
			fn, ok := getRule(rule.Tag)
			if !ok {
				continue
			}
			if msg := fn(fullName, fieldVal, rule.Param); msg != "" {
				code := strings.ToUpper(rule.Tag)
				builder.Add(fullName, code, msg)
			}
		}
	}
}

// ── Built-in rule implementations ──────────────────────────────────────────

func ruleRequired(fieldName string, v reflect.Value, param string) string {
	if !v.IsValid() {
		return fmt.Sprintf("%s is required", fieldName)
	}
	switch v.Kind() {
	case reflect.String:
		if v.String() == "" {
			return fmt.Sprintf("%s is required", fieldName)
		}
	case reflect.Slice, reflect.Map, reflect.Array:
		if v.Len() == 0 {
			return fmt.Sprintf("%s is required", fieldName)
		}
	case reflect.Ptr, reflect.Interface:
		if v.IsNil() {
			return fmt.Sprintf("%s is required", fieldName)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		// Zero is a valid value for numbers; use min=1 to require non-zero
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		// Same as above
	case reflect.Float32, reflect.Float64:
		// Same as above
	case reflect.Bool:
		// false is a valid value
	}
	return ""
}

func ruleMin(fieldName string, v reflect.Value, param string) string {
	if param == "" {
		return ""
	}
	minVal, err := strconv.ParseFloat(param, 64)
	if err != nil {
		return ""
	}
	switch v.Kind() {
	case reflect.String:
		if float64(len([]rune(v.String()))) < minVal {
			return fmt.Sprintf("%s must be at least %s characters", fieldName, param)
		}
	case reflect.Slice, reflect.Array:
		if float64(v.Len()) < minVal {
			return fmt.Sprintf("%s must have at least %s items", fieldName, param)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if float64(v.Int()) < minVal {
			return fmt.Sprintf("%s must be at least %s", fieldName, param)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if float64(v.Uint()) < minVal {
			return fmt.Sprintf("%s must be at least %s", fieldName, param)
		}
	case reflect.Float32, reflect.Float64:
		if v.Float() < minVal {
			return fmt.Sprintf("%s must be at least %s", fieldName, param)
		}
	case reflect.Map:
		if float64(v.Len()) < minVal {
			return fmt.Sprintf("%s must have at least %s entries", fieldName, param)
		}
	}
	return ""
}

func ruleMax(fieldName string, v reflect.Value, param string) string {
	if param == "" {
		return ""
	}
	maxVal, err := strconv.ParseFloat(param, 64)
	if err != nil {
		return ""
	}
	switch v.Kind() {
	case reflect.String:
		if float64(len([]rune(v.String()))) > maxVal {
			return fmt.Sprintf("%s must be at most %s characters", fieldName, param)
		}
	case reflect.Slice, reflect.Array:
		if float64(v.Len()) > maxVal {
			return fmt.Sprintf("%s must have at most %s items", fieldName, param)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if float64(v.Int()) > maxVal {
			return fmt.Sprintf("%s must be at most %s", fieldName, param)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if float64(v.Uint()) > maxVal {
			return fmt.Sprintf("%s must be at most %s", fieldName, param)
		}
	case reflect.Float32, reflect.Float64:
		if v.Float() > maxVal {
			return fmt.Sprintf("%s must be at most %s", fieldName, param)
		}
	case reflect.Map:
		if float64(v.Len()) > maxVal {
			return fmt.Sprintf("%s must have at most %s entries", fieldName, param)
		}
	}
	return ""
}

func ruleLen(fieldName string, v reflect.Value, param string) string {
	if param == "" {
		return ""
	}
	lenVal, err := strconv.ParseInt(param, 10, 64)
	if err != nil {
		return ""
	}
	switch v.Kind() {
	case reflect.String:
		if int64(len([]rune(v.String()))) != lenVal {
			return fmt.Sprintf("%s must be exactly %s characters", fieldName, param)
		}
	case reflect.Slice, reflect.Array:
		if int64(v.Len()) != lenVal {
			return fmt.Sprintf("%s must have exactly %s items", fieldName, param)
		}
	case reflect.Map:
		if int64(v.Len()) != lenVal {
			return fmt.Sprintf("%s must have exactly %s entries", fieldName, param)
		}
	}
	return ""
}

func ruleEq(fieldName string, v reflect.Value, param string) string {
	switch v.Kind() {
	case reflect.String:
		if v.String() != param {
			return fmt.Sprintf("%s must be %s", fieldName, param)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(param, 10, 64)
		if err == nil && v.Int() != n {
			return fmt.Sprintf("%s must be %s", fieldName, param)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(param, 10, 64)
		if err == nil && v.Uint() != n {
			return fmt.Sprintf("%s must be %s", fieldName, param)
		}
	case reflect.Float32, reflect.Float64:
		n, err := strconv.ParseFloat(param, 64)
		if err == nil && v.Float() != n {
			return fmt.Sprintf("%s must be %s", fieldName, param)
		}
	case reflect.Bool:
		b, err := strconv.ParseBool(param)
		if err == nil && v.Bool() != b {
			return fmt.Sprintf("%s must be %s", fieldName, param)
		}
	}
	return ""
}

func ruleNeq(fieldName string, v reflect.Value, param string) string {
	switch v.Kind() {
	case reflect.String:
		if v.String() == param {
			return fmt.Sprintf("%s must not be %s", fieldName, param)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(param, 10, 64)
		if err == nil && v.Int() == n {
			return fmt.Sprintf("%s must not be %s", fieldName, param)
		}
	}
	return ""
}

func ruleGt(fieldName string, v reflect.Value, param string) string {
	n, err := strconv.ParseFloat(param, 64)
	if err != nil {
		return ""
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if float64(v.Int()) <= n {
			return fmt.Sprintf("%s must be greater than %s", fieldName, param)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if float64(v.Uint()) <= n {
			return fmt.Sprintf("%s must be greater than %s", fieldName, param)
		}
	case reflect.Float32, reflect.Float64:
		if v.Float() <= n {
			return fmt.Sprintf("%s must be greater than %s", fieldName, param)
		}
	}
	return ""
}

func ruleGte(fieldName string, v reflect.Value, param string) string {
	n, err := strconv.ParseFloat(param, 64)
	if err != nil {
		return ""
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if float64(v.Int()) < n {
			return fmt.Sprintf("%s must be at least %s", fieldName, param)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if float64(v.Uint()) < n {
			return fmt.Sprintf("%s must be at least %s", fieldName, param)
		}
	case reflect.Float32, reflect.Float64:
		if v.Float() < n {
			return fmt.Sprintf("%s must be at least %s", fieldName, param)
		}
	}
	return ""
}

func ruleLt(fieldName string, v reflect.Value, param string) string {
	n, err := strconv.ParseFloat(param, 64)
	if err != nil {
		return ""
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if float64(v.Int()) >= n {
			return fmt.Sprintf("%s must be less than %s", fieldName, param)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if float64(v.Uint()) >= n {
			return fmt.Sprintf("%s must be less than %s", fieldName, param)
		}
	case reflect.Float32, reflect.Float64:
		if v.Float() >= n {
			return fmt.Sprintf("%s must be less than %s", fieldName, param)
		}
	}
	return ""
}

func ruleLte(fieldName string, v reflect.Value, param string) string {
	n, err := strconv.ParseFloat(param, 64)
	if err != nil {
		return ""
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if float64(v.Int()) > n {
			return fmt.Sprintf("%s must be at most %s", fieldName, param)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if float64(v.Uint()) > n {
			return fmt.Sprintf("%s must be at most %s", fieldName, param)
		}
	case reflect.Float32, reflect.Float64:
		if v.Float() > n {
			return fmt.Sprintf("%s must be at most %s", fieldName, param)
		}
	}
	return ""
}

func ruleOneof(fieldName string, v reflect.Value, param string) string {
	options := strings.Split(param, " ")
	if len(options) == 0 {
		return ""
	}
	s := fmt.Sprintf("%v", v.Interface())
	for _, opt := range options {
		if s == opt {
			return ""
		}
	}
	return fmt.Sprintf("%s must be one of: %s", fieldName, strings.Join(options, ", "))
}

func ruleEmail(fieldName string, v reflect.Value, param string) string {
	if v.Kind() != reflect.String {
		return ""
	}
	s := v.String()
	if s == "" {
		return ""
	}
	if !strings.Contains(s, "@") || !strings.Contains(s, ".") {
		return fmt.Sprintf("%s must be a valid email address", fieldName)
	}
	parts := strings.Split(s, "@")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return fmt.Sprintf("%s must be a valid email address", fieldName)
	}
	return ""
}

func ruleURL(fieldName string, v reflect.Value, param string) string {
	if v.Kind() != reflect.String {
		return ""
	}
	s := v.String()
	if s == "" {
		return ""
	}
	if !strings.HasPrefix(s, "http://") && !strings.HasPrefix(s, "https://") {
		return fmt.Sprintf("%s must be a valid URL", fieldName)
	}
	return ""
}

func ruleURI(fieldName string, v reflect.Value, param string) string {
	if v.Kind() != reflect.String {
		return ""
	}
	s := v.String()
	if s == "" {
		return ""
	}
	if !strings.HasPrefix(s, "/") && !strings.HasPrefix(s, "http://") && !strings.HasPrefix(s, "https://") {
		return fmt.Sprintf("%s must be a valid URI", fieldName)
	}
	return ""
}

var uuidRegex = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func ruleUUID(fieldName string, v reflect.Value, param string) string {
	if v.Kind() != reflect.String {
		return ""
	}
	s := v.String()
	if s == "" {
		return ""
	}
	if !uuidRegex.MatchString(s) {
		return fmt.Sprintf("%s must be a valid UUID", fieldName)
	}
	return ""
}

func ruleIP(fieldName string, v reflect.Value, param string) string {
	if v.Kind() != reflect.String {
		return ""
	}
	s := v.String()
	if s == "" {
		return ""
	}
	if netIP := parseIP(s); netIP == nil {
		return fmt.Sprintf("%s must be a valid IP address", fieldName)
	}
	return ""
}

func ruleIPv4(fieldName string, v reflect.Value, param string) string {
	if v.Kind() != reflect.String {
		return ""
	}
	s := v.String()
	if s == "" {
		return ""
	}
	parts := strings.Split(s, ".")
	if len(parts) != 4 {
		return fmt.Sprintf("%s must be a valid IPv4 address", fieldName)
	}
	for _, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 || n > 255 {
			return fmt.Sprintf("%s must be a valid IPv4 address", fieldName)
		}
	}
	return ""
}

func ruleIPv6(fieldName string, v reflect.Value, param string) string {
	if v.Kind() != reflect.String {
		return ""
	}
	s := v.String()
	if s == "" {
		return ""
	}
	if !strings.Contains(s, ":") {
		return fmt.Sprintf("%s must be a valid IPv6 address", fieldName)
	}
	return ""
}

func ruleCIDR(fieldName string, v reflect.Value, param string) string {
	if v.Kind() != reflect.String {
		return ""
	}
	s := v.String()
	if s == "" {
		return ""
	}
	parts := strings.Split(s, "/")
	if len(parts) != 2 {
		return fmt.Sprintf("%s must be a valid CIDR notation", fieldName)
	}
	if netIP := parseIP(parts[0]); netIP == nil {
		return fmt.Sprintf("%s must be a valid CIDR notation", fieldName)
	}
	_, err := strconv.Atoi(parts[1])
	if err != nil {
		return fmt.Sprintf("%s must be a valid CIDR notation", fieldName)
	}
	return ""
}

func ruleMAC(fieldName string, v reflect.Value, param string) string {
	if v.Kind() != reflect.String {
		return ""
	}
	s := v.String()
	if s == "" {
		return ""
	}
	parts := strings.Split(s, ":")
	if len(parts) != 6 {
		return fmt.Sprintf("%s must be a valid MAC address", fieldName)
	}
	for _, p := range parts {
		if len(p) != 2 {
			return fmt.Sprintf("%s must be a valid MAC address", fieldName)
		}
	}
	return ""
}

func ruleAlpha(fieldName string, v reflect.Value, param string) string {
	if v.Kind() != reflect.String {
		return ""
	}
	s := v.String()
	if s == "" {
		return ""
	}
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
			return fmt.Sprintf("%s must contain only letters", fieldName)
		}
	}
	return ""
}

func ruleAlphanum(fieldName string, v reflect.Value, param string) string {
	if v.Kind() != reflect.String {
		return ""
	}
	s := v.String()
	if s == "" {
		return ""
	}
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
			return fmt.Sprintf("%s must contain only letters and numbers", fieldName)
		}
	}
	return ""
}

func ruleNumeric(fieldName string, v reflect.Value, param string) string {
	if v.Kind() != reflect.String {
		return ""
	}
	s := v.String()
	if s == "" {
		return ""
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || c == '-' || c == '.') {
			return fmt.Sprintf("%s must be numeric", fieldName)
		}
	}
	return ""
}

func ruleBoolean(fieldName string, v reflect.Value, param string) string {
	if v.Kind() == reflect.Bool {
		return ""
	}
	if v.Kind() != reflect.String {
		return ""
	}
	s := v.String()
	if s == "" {
		return ""
	}
	_, err := strconv.ParseBool(s)
	if err != nil {
		return fmt.Sprintf("%s must be a boolean value", fieldName)
	}
	return ""
}

func ruleJSON(fieldName string, v reflect.Value, param string) string {
	if v.Kind() != reflect.String {
		return ""
	}
	s := v.String()
	if s == "" {
		return ""
	}
	if !json.Valid([]byte(s)) {
		return fmt.Sprintf("%s must be valid JSON", fieldName)
	}
	return ""
}

func ruleDatetime(fieldName string, v reflect.Value, param string) string {
	if v.Kind() != reflect.String {
		return ""
	}
	s := v.String()
	if s == "" {
		return ""
	}
	layout := param
	if layout == "" {
		layout = time.RFC3339
	}
	_, err := time.Parse(layout, s)
	if err != nil {
		return fmt.Sprintf("%s must be a valid datetime (format: %s)", fieldName, layout)
	}
	return ""
}

func ruleEqField(fieldName string, v reflect.Value, param string) string {
	// This is handled at the struct level in validateStructFields
	return ""
}

func ruleNeqField(fieldName string, v reflect.Value, param string) string {
	return ""
}

func ruleGtField(fieldName string, v reflect.Value, param string) string {
	return ""
}

func ruleGteField(fieldName string, v reflect.Value, param string) string {
	return ""
}

func ruleLtField(fieldName string, v reflect.Value, param string) string {
	return ""
}

func ruleLteField(fieldName string, v reflect.Value, param string) string {
	return ""
}

func ruleContains(fieldName string, v reflect.Value, param string) string {
	if v.Kind() != reflect.String {
		return ""
	}
	if !strings.Contains(v.String(), param) {
		return fmt.Sprintf("%s must contain %q", fieldName, param)
	}
	return ""
}

func ruleExcludes(fieldName string, v reflect.Value, param string) string {
	if v.Kind() != reflect.String {
		return ""
	}
	if strings.Contains(v.String(), param) {
		return fmt.Sprintf("%s must not contain %q", fieldName, param)
	}
	return ""
}

func ruleStartswith(fieldName string, v reflect.Value, param string) string {
	if v.Kind() != reflect.String {
		return ""
	}
	if !strings.HasPrefix(v.String(), param) {
		return fmt.Sprintf("%s must start with %q", fieldName, param)
	}
	return ""
}

func ruleEndswith(fieldName string, v reflect.Value, param string) string {
	if v.Kind() != reflect.String {
		return ""
	}
	if !strings.HasSuffix(v.String(), param) {
		return fmt.Sprintf("%s must end with %q", fieldName, param)
	}
	return ""
}

func rulePrefix(fieldName string, v reflect.Value, param string) string {
	return ruleStartswith(fieldName, v, param)
}

func ruleSuffix(fieldName string, v reflect.Value, param string) string {
	return ruleEndswith(fieldName, v, param)
}

func ruleLenMin(fieldName string, v reflect.Value, param string) string {
	return ruleMin(fieldName, v, param)
}

func ruleLenMax(fieldName string, v reflect.Value, param string) string {
	return ruleMax(fieldName, v, param)
}

// parseIP is a simple IP parser (IPv4 and IPv6).
func parseIP(s string) []byte {
	if strings.Contains(s, ".") {
		parts := strings.Split(s, ".")
		if len(parts) != 4 {
			return nil
		}
		var ip [4]byte
		for i, p := range parts {
			n, err := strconv.Atoi(p)
			if err != nil || n < 0 || n > 255 {
				return nil
			}
			ip[i] = byte(n)
		}
		return ip[:]
	}
	return nil // simplified; full IPv6 parsing not shown
}

// ── Cross-field validation ─────────────────────────────────────────────────

// validateCrossFields checks rules that reference other fields.
// Must be called after all struct fields are parsed.
func validateCrossFields(rv reflect.Value, prefix string, builder *ValidationErrorBuilder) {
	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		fieldVal := rv.Field(i)

		if !field.IsExported() {
			continue
		}

		fieldName := field.Name
		jsonTag := field.Tag.Get("json")
		if jsonTag == "-" {
			continue
		}
		if jsonTag != "" {
			if idx := strings.IndexByte(jsonTag, ','); idx >= 0 {
				jsonTag = jsonTag[:idx]
			}
			if jsonTag != "" {
				fieldName = jsonTag
			}
		}

		fullName := fieldName
		if prefix != "" {
			fullName = prefix + "." + fieldName
		}

		// Handle pointer/struct recursion
		fv := fieldVal
		if fv.Kind() == reflect.Ptr {
			if fv.IsNil() {
				continue
			}
			fv = fv.Elem()
		}
		if fv.Kind() == reflect.Struct && fv.Type() != reflect.TypeOf(time.Time{}) {
			validateCrossFields(fv, fullName, builder)
			continue
		}

		// Get validation tag
		var tag string
		for _, tagName := range validateTagNames {
			if t := field.Tag.Get(tagName); t != "" {
				tag = t
				break
			}
		}
		if tag == "" {
			continue
		}

		rules := parseValidateTag(tag)
		for _, rule := range rules {
			switch rule.Tag {
			case "eqfield", "neqfield", "gtfield", "gtefield", "ltfield", "ltefield":
				otherField := rv.FieldByName(rule.Param)
				if !otherField.IsValid() {
					continue
				}
				msg := ""
				switch rule.Tag {
				case "eqfield":
					if fmt.Sprintf("%v", fv.Interface()) != fmt.Sprintf("%v", otherField.Interface()) {
						msg = fmt.Sprintf("%s must be equal to %s", fullName, rule.Param)
					}
				case "neqfield":
					if fmt.Sprintf("%v", fv.Interface()) == fmt.Sprintf("%v", otherField.Interface()) {
						msg = fmt.Sprintf("%s must not be equal to %s", fullName, rule.Param)
					}
				}
				if msg != "" {
					builder.Add(fullName, strings.ToUpper(rule.Tag), msg)
				}
			}
		}
	}
}

// ── Validation middleware ──────────────────────────────────────────────────

// ValidateConfig configures the validation middleware.
type ValidateConfig struct {
	// Skip allows skipping validation for specific requests.
	Skip func(Ctx) bool
}

// Validate returns a middleware that validates request body using the
// provided Validator factory. The factory creates a new validator for each
// request, which is then decoded into and validated.
//
// Usage:
//
//	type CreateUserRequest struct {
//	    Name  string `json:"name" validate:"required,min=2,max=100"`
//	    Email string `json:"email" validate:"required,email"`
//	}
//
//	app.Post("/users", validate.Middleware(func() fh.Validator {
//	    return &CreateUserRequest{}
//	}), createUserHandler)
func ValidateMiddleware(factory func() Validator, cfg ...ValidateConfig) HandlerFunc {
	var skip func(Ctx) bool
	if len(cfg) > 0 && cfg[0].Skip != nil {
		skip = cfg[0].Skip
	}

	return func(c Ctx) error {
		if skip != nil && skip(c) {
			return c.Next()
		}

		v := factory()
		if v == nil {
			return c.Next()
		}

		// Try to decode body into the validator
		if err := c.BodyParser(v); err != nil {
			return BadRequest("Invalid request body")
		}

		// Run struct tag validation
		if err := ValidateStruct(v); err != nil {
			return err
		}

		// Run custom validation
		if err := v.Validate(); err != nil {
			return err
		}

		return c.Next()
	}
}

// ValidateQuery returns a middleware that validates query parameters using
// the provided struct. The struct should have "query" tags for binding.
func ValidateQuery(v any, cfg ...ValidateConfig) HandlerFunc {
	var skip func(Ctx) bool
	if len(cfg) > 0 && cfg[0].Skip != nil {
		skip = cfg[0].Skip
	}

	return func(c Ctx) error {
		if skip != nil && skip(c) {
			return c.Next()
		}

		if err := c.QueryParser(v); err != nil {
			return BadRequest("Invalid query parameters")
		}

		if err := ValidateStruct(v); err != nil {
			return err
		}

		if validator, ok := v.(Validator); ok {
			if err := validator.Validate(); err != nil {
				return err
			}
		}

		return c.Next()
	}
}

// ValidateHeaders returns a middleware that validates request headers using
// the provided struct. The struct should have "header" tags for binding.
func ValidateHeaders(v any, cfg ...ValidateConfig) HandlerFunc {
	var skip func(Ctx) bool
	if len(cfg) > 0 && cfg[0].Skip != nil {
		skip = cfg[0].Skip
	}

	return func(c Ctx) error {
		if skip != nil && skip(c) {
			return c.Next()
		}

		if err := c.HeaderParser(v); err != nil {
			return BadRequest("Invalid request headers")
		}

		if err := ValidateStruct(v); err != nil {
			return err
		}

		if validator, ok := v.(Validator); ok {
			if err := validator.Validate(); err != nil {
				return err
			}
		}

		return c.Next()
	}
}
