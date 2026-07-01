package apikey

import "testing"

func TestGenerateHashAndSplitKey(t *testing.T) {
	key, hash, err := Generate("fh_test", 16)
	if err != nil {
		t.Fatal(err)
	}
	id, secret := SplitKey(key)
	if id != "fh_test" || secret == "" {
		t.Fatalf("bad split: %q %q", id, secret)
	}
	if !ConstantTimeHashEqual(key, hash) {
		t.Fatal("hash mismatch")
	}
	if ConstantTimeHashEqual(key+"x", hash) {
		t.Fatal("unexpected match")
	}
}
func TestMemoryStoreRevoke(t *testing.T) {
	key, hash, _ := Generate("fh_live", 8)
	store := NewMemoryStore(KeyRecord{ID: "fh_live", Hash: hash})
	rec, ok, err := store.Lookup(nil, "fh_live")
	if err != nil || !ok {
		t.Fatal("lookup failed")
	}
	if !VerifyRecord(nil, key, rec) {
		t.Fatal("expected verify")
	}
	if err := store.Revoke("fh_live"); err != nil {
		t.Fatal(err)
	}
	rec, _, _ = store.Lookup(nil, "fh_live")
	if VerifyRecord(nil, key, rec) {
		t.Fatal("revoked verified")
	}
}
