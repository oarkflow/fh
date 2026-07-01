package admin

import "testing"

func TestAdminTrim(t *testing.T) {
	if got := trim("/_fh/admin///"); got != "/_fh/admin" {
		t.Fatalf("trim=%q", got)
	}
}
func TestStaticToken(t *testing.T) {
	if StaticToken("X", "y") == nil {
		t.Fatal("nil auth")
	}
}
