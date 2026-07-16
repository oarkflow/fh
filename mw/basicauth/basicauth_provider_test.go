package basicauth

import (
	"context"
	"crypto/tls"
	"testing"

	"github.com/oarkflow/fh"
)

type tlsTestCtx struct {
	fh.Ctx
	ctx context.Context
}

func (c *tlsTestCtx) Context() context.Context { return c.ctx }

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

func TestRequireTLSRecognizesNativeTLSState(t *testing.T) {
	ctx := &tlsTestCtx{ctx: fh.WithTLSState(context.Background(), tls.ConnectionState{Version: tls.VersionTLS13})}
	if !isHTTPS(ctx, nil) {
		t.Fatal("native TLS state was not recognized")
	}
}
