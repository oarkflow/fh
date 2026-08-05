package fh

import "testing"

// TestRedactMapNestedSlices guards against a regression where secrets nested
// inside arrays (a common shape for audit metadata, e.g. a list of user
// records) were passed through RedactMap unredacted. Only direct map values
// were ever inspected before this fix.
func TestRedactMapNestedSlices(t *testing.T) {
	r := NewRedactor(DefaultRedactionConfig())

	in := map[string]any{
		"users": []any{
			map[string]any{"username": "alice", "password": "hunter2"},
			map[string]any{"username": "bob", "api_key": "sk_live_abc123"},
		},
		"records": []map[string]any{
			{"token": "abc.def.ghi", "note": "fine"},
		},
		"plain": "no secrets here",
	}

	out := r.RedactMap(in)

	users, ok := out["users"].([]any)
	if !ok || len(users) != 2 {
		t.Fatalf("expected 2 users, got %#v", out["users"])
	}
	u0, ok := users[0].(map[string]any)
	if !ok || u0["password"] != r.Replacement {
		t.Fatalf("expected password redacted in nested slice element, got %#v", users[0])
	}
	if u0["username"] != "alice" {
		t.Fatalf("expected non-sensitive field preserved, got %#v", u0["username"])
	}
	u1, ok := users[1].(map[string]any)
	if !ok || u1["api_key"] != r.Replacement {
		t.Fatalf("expected api_key redacted in nested slice element, got %#v", users[1])
	}

	records, ok := out["records"].([]map[string]any)
	if !ok || len(records) != 1 {
		t.Fatalf("expected 1 record, got %#v", out["records"])
	}
	if records[0]["token"] != r.Replacement {
		t.Fatalf("expected token redacted in []map[string]any, got %#v", records[0]["token"])
	}
	if records[0]["note"] != "fine" {
		t.Fatalf("expected non-sensitive field preserved, got %#v", records[0]["note"])
	}

	if out["plain"] != "no secrets here" {
		t.Fatalf("expected non-sensitive top-level string preserved, got %#v", out["plain"])
	}
}

func TestRedactMapNilAndSensitiveKey(t *testing.T) {
	r := NewRedactor(DefaultRedactionConfig())
	if r.RedactMap(nil) != nil {
		t.Fatal("expected nil map to redact to nil")
	}
	out := r.RedactMap(map[string]any{"Authorization": "Bearer xyz"})
	if out["Authorization"] != r.Replacement {
		t.Fatalf("expected case-insensitive sensitive key match, got %#v", out["Authorization"])
	}
}
