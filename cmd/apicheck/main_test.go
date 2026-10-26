package main

import "testing"

func TestDeclarationsIgnorePrivateFieldsButDetectPublicSignatures(t *testing.T) {
	before := map[string][]byte{
		"go.mod": nil,
		"api.go": []byte("package api\ntype Client struct { Public string; private int }\nfunc (c *Client) Send(ctx string) error { return nil }\n"),
	}
	after := map[string][]byte{
		"go.mod": nil,
		"api.go": []byte("package api\ntype Client struct { Public string; private bool }\nfunc (c *Client) Send(context string) error { return nil }\n"),
	}
	oldAPI, err := declarations(before)
	if err != nil {
		t.Fatal(err)
	}
	newAPI, err := declarations(after)
	if err != nil {
		t.Fatal(err)
	}
	for key, old := range oldAPI {
		if old != newAPI[key] {
			t.Fatalf("private field or parameter name changed %s: %q -> %q", key, old, newAPI[key])
		}
	}
	after["api.go"] = []byte("package api\ntype Client struct { Public int; private bool }\nfunc (c *Client) Send(context int) error { return nil }\n")
	changed, err := declarations(after)
	if err != nil {
		t.Fatal(err)
	}
	if changed[".Client"] == oldAPI[".Client"] || changed[".Client.Send"] == oldAPI[".Client.Send"] {
		t.Fatalf("exported API change was not detected: before=%v after=%v", oldAPI, changed)
	}
}
