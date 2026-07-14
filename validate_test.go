package fh

import (
	"reflect"
	"testing"
)

func TestValidateStruct(t *testing.T) {
	type User struct {
		Name  string `json:"name" validate:"required,min=2,max=100"`
		Email string `json:"email" validate:"required,email"`
		Age   int    `json:"age" validate:"min=0,max=150"`
	}

	// Valid
	u := User{Name: "Alice", Email: "alice@example.com", Age: 30}
	if err := ValidateStruct(u); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Missing name (fails both required and min)
	u2 := User{Name: "", Email: "alice@example.com", Age: 30}
	err := ValidateStruct(u2)
	if err == nil {
		t.Fatal("expected error for missing name")
	}
	ve, ok := err.(*ValidationError)
	if !ok {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if !ValidationErrors(ve.Fields).Has("name") {
		t.Fatalf("expected name field error, got %v", ve.Fields)
	}

	// Invalid email
	u3 := User{Name: "Alice", Email: "not-an-email", Age: 30}
	err = ValidateStruct(u3)
	if err == nil {
		t.Fatal("expected error for invalid email")
	}
	ve = err.(*ValidationError)
	if len(ve.Fields) != 1 || ve.Fields[0].Code != "EMAIL" {
		t.Fatalf("expected EMAIL code, got %v", ve.Fields[0].Code)
	}

	// Multiple errors
	u4 := User{Name: "", Email: "", Age: 200}
	err = ValidateStruct(u4)
	if err == nil {
		t.Fatal("expected multiple errors")
	}
	ve = err.(*ValidationError)
	if len(ve.Fields) < 2 {
		t.Fatalf("expected at least 2 errors, got %d", len(ve.Fields))
	}
}

func TestValidateStructNested(t *testing.T) {
	type Address struct {
		City string `json:"city" validate:"required"`
	}
	type User struct {
		Name    string  `json:"name" validate:"required"`
		Address Address `json:"address"`
	}

	u := User{Name: "Alice", Address: Address{City: ""}}
	err := ValidateStruct(u)
	if err == nil {
		t.Fatal("expected error for missing city")
	}
	ve := err.(*ValidationError)
	if len(ve.Fields) != 1 || ve.Fields[0].Field != "address.city" {
		t.Fatalf("expected address.city field, got %v", ve.Fields)
	}
}

func TestValidateOneof(t *testing.T) {
	type Status struct {
		State string `json:"state" validate:"oneof=active inactive pending"`
	}

	s := Status{State: "deleted"}
	err := ValidateStruct(s)
	if err == nil {
		t.Fatal("expected error for invalid oneof")
	}
	ve := err.(*ValidationError)
	if ve.Fields[0].Code != "ONEOF" {
		t.Fatalf("expected ONEOF code, got %v", ve.Fields[0].Code)
	}

	s2 := Status{State: "active"}
	if err := ValidateStruct(s2); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestValidateURL(t *testing.T) {
	type Link struct {
		URL string `json:"url" validate:"url"`
	}

	l := Link{URL: "https://example.com"}
	if err := ValidateStruct(l); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	l2 := Link{URL: "not-a-url"}
	if err := ValidateStruct(l2); err == nil {
		t.Fatal("expected error for invalid URL")
	}
}

func TestValidateUUID(t *testing.T) {
	type Record struct {
		ID string `json:"id" validate:"uuid"`
	}

	r := Record{ID: "550e8400-e29b-41d4-a716-446655440000"}
	if err := ValidateStruct(r); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	r2 := Record{ID: "not-a-uuid"}
	if err := ValidateStruct(r2); err == nil {
		t.Fatal("expected error for invalid UUID")
	}
}

func TestValidatePattern(t *testing.T) {
	type Code struct {
		Value string `json:"value" validate:"len=6,numeric"`
	}

	c := Code{Value: "123456"}
	if err := ValidateStruct(c); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	c2 := Code{Value: "abc"}
	if err := ValidateStruct(c2); err == nil {
		t.Fatal("expected error for non-numeric")
	}
}

func TestValidationErrorsHas(t *testing.T) {
	errs := ValidationErrors{
		{Field: "name", Code: "REQUIRED", Message: "name is required"},
		{Field: "email", Code: "EMAIL", Message: "email is invalid"},
	}

	if !errs.Has("name") {
		t.Fatal("expected Has('name') to be true")
	}
	if errs.Has("age") {
		t.Fatal("expected Has('age') to be false")
	}
}

func TestValidationErrorsGet(t *testing.T) {
	errs := ValidationErrors{
		{Field: "name", Code: "REQUIRED", Message: "name is required"},
		{Field: "email", Code: "EMAIL", Message: "email is invalid"},
		{Field: "name", Code: "MIN", Message: "name too short"},
	}

	nameErrs := errs.Get("name")
	if len(nameErrs) != 2 {
		t.Fatalf("expected 2 name errors, got %d", len(nameErrs))
	}
}

func TestValidationErrorBuilder(t *testing.T) {
	b := NewValidationErrorBuilder()
	b.Add("name", "REQUIRED", "name is required")
	b.Addf("email", "EMAIL", "email %q is invalid", "bad")

	if !b.HasErrors() {
		t.Fatal("expected HasErrors to be true")
	}

	err := b.Error()
	if err == nil {
		t.Fatal("expected error")
	}
	if len(err.Fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(err.Fields))
	}

	b.Clear()
	if b.HasErrors() {
		t.Fatal("expected HasErrors to be false after Clear")
	}
}

func TestRegisterRule(t *testing.T) {
	RegisterRule("always_fail", func(fieldName string, v reflect.Value, param string) string {
		return "always fails"
	})

	type Test struct {
		Value string `validate:"always_fail"`
	}

	err := ValidateStruct(Test{Value: "anything"})
	if err == nil {
		t.Fatal("expected error from custom rule")
	}
	ve := err.(*ValidationError)
	if ve.Fields[0].Code != "ALWAYS_FAIL" {
		t.Fatalf("expected ALWAYS_FAIL code, got %v", ve.Fields[0].Code)
	}
}

func TestValidateStructPtr(t *testing.T) {
	type User struct {
		Name string `validate:"required"`
	}

	u := &User{Name: ""}
	if err := ValidateStruct(u); err == nil {
		t.Fatal("expected error")
	}

	u2 := &User{Name: "Alice"}
	if err := ValidateStruct(u2); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Nil pointer
	var u3 *User
	if err := ValidateStruct(u3); err != nil {
		t.Fatalf("expected no error for nil pointer, got %v", err)
	}
}

func TestValidateCrossField(t *testing.T) {
	type Password struct {
		Password  string `json:"password" validate:"required,min=8"`
		Confirm   string `json:"confirm" validate:"eqfield=Password"`
	}

	p := Password{Password: "secret123", Confirm: "different"}
	err := ValidateStruct(p)
	if err == nil {
		t.Fatal("expected error for mismatched passwords")
	}
	ve := err.(*ValidationError)
	found := false
	for _, f := range ve.Fields {
		if f.Field == "confirm" && f.Code == "EQFIELD" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected confirm/EQFIELD error, got %v", ve.Fields)
	}
}

func TestValidateDatetime(t *testing.T) {
	type Event struct {
		Date string `validate:"datetime=2006-01-02"`
	}

	e := Event{Date: "2024-01-15"}
	if err := ValidateStruct(e); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	e2 := Event{Date: "not-a-date"}
	if err := ValidateStruct(e2); err == nil {
		t.Fatal("expected error for invalid date")
	}
}

func TestValidateIP(t *testing.T) {
	type Server struct {
		IP string `validate:"ipv4"`
	}

	s := Server{IP: "192.168.1.1"}
	if err := ValidateStruct(s); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	s2 := Server{IP: "999.999.999.999"}
	if err := ValidateStruct(s2); err == nil {
		t.Fatal("expected error for invalid IP")
	}
}

func TestValidateJSON(t *testing.T) {
	type Payload struct {
		Data string `validate:"json"`
	}

	p := Payload{Data: `{"key":"value"}`}
	if err := ValidateStruct(p); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	p2 := Payload{Data: "not json"}
	if err := ValidateStruct(p2); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestParseValidateTag(t *testing.T) {
	rules := parseValidateTag("required,min=1,max=100,email")
	if len(rules) != 4 {
		t.Fatalf("expected 4 rules, got %d", len(rules))
	}
	if rules[0].Tag != "required" || rules[0].Param != "" {
		t.Fatalf("expected required rule, got %v", rules[0])
	}
	if rules[1].Tag != "min" || rules[1].Param != "1" {
		t.Fatalf("expected min=1 rule, got %v", rules[1])
	}
	if rules[3].Tag != "email" || rules[3].Param != "" {
		t.Fatalf("expected email rule, got %v", rules[3])
	}
}

func TestValidateAlpha(t *testing.T) {
	type Name struct {
		Value string `validate:"alpha"`
	}

	if err := ValidateStruct(Name{Value: "abc"}); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if err := ValidateStruct(Name{Value: "abc123"}); err == nil {
		t.Fatal("expected error for non-alpha")
	}
}

func TestValidateAlphanum(t *testing.T) {
	type Code struct {
		Value string `validate:"alphanum"`
	}

	if err := ValidateStruct(Code{Value: "abc123"}); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if err := ValidateStruct(Code{Value: "abc-123"}); err == nil {
		t.Fatal("expected error for non-alphanum")
	}
}
