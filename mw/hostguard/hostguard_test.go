package hostguard

import "testing"

func TestNormalize(t *testing.T) {
	if normalize("Example.COM:443") != "example.com" {
		t.Fatal(normalize("Example.COM:443"))
	}
}
