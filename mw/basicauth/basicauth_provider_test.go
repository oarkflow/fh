package basicauth

import "testing"

func TestUsersProviderStorage(t *testing.T) {
	hash, err := HashPassword("secret")
	if err != nil {
		t.Fatal(err)
	}
	store := ProviderStorage{Provider: func() ([]User, error) {
		return []User{{Username: "alice", PasswordHash: hash, Enabled: true, Roles: []string{"admin"}}}, nil
	}}
	user, ok, err := store.GetUser("alice")
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if user.Username != "alice" || len(user.Roles) != 1 || user.Roles[0] != "admin" {
		t.Fatalf("unexpected user: %#v", user)
	}
}

func TestPlainUsersProviderStorageAndHasher(t *testing.T) {
	store := PlainProviderStorage{Provider: func() ([]PlainUser, error) {
		return []PlainUser{{Username: "alice", Password: "secret"}}, nil
	}}
	user, ok, err := store.GetUser("alice")
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	hasher := plainAwareHasher{PasswordHasher: DefaultHasher()}
	if !hasher.Verify("secret", user.PasswordHash) {
		t.Fatal("expected plaintext provider password to verify")
	}
	if hasher.Verify("wrong", user.PasswordHash) {
		t.Fatal("wrong password verified")
	}
}

func TestNewFromPlainUsers(t *testing.T) {
	mw, err := NewFromPlainUsers(map[string]string{"alice": "secret", "bob": "hunter2"})
	if err != nil {
		t.Fatal(err)
	}
	if mw == nil {
		t.Fatal("nil middleware")
	}
}
