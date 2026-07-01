package cors

import "testing"

func TestCORSRejectsWildcardCredentials(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = New(Config{AllowOrigins: []string{"*"}, AllowCredentials: true})
}

func TestValidOriginRejectsCRLF(t *testing.T) {
	if validOrigin("https://example.com\r\nX: y") {
		t.Fatal("origin with CRLF should be invalid")
	}
}
