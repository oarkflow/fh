package access

import "testing"

func TestParseACL(t *testing.T) {
	got, err := Parse("# comment\ndevice-1\ndevice-2, device-3")
	if err != nil || len(got) != 3 {
		t.Fatal(got, err)
	}
	for _, raw := range []string{"", "# comment only", "foo bar", "a\x00b"} {
		if _, err := Parse(raw); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
}
