package session

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

func TestCookieSecurityValidation(t *testing.T) {
	valid := &fh.Cookie{Name: "__Host-session", Value: "abc_123", Path: "/", Secure: true, HttpOnly: true, SameSite: fh.SameSiteStrict, Partitioned: true}
	if err := valid.Valid(); err != nil {
		t.Fatal(err)
	}
	serialized := valid.String()
	if !strings.Contains(serialized, "; Secure") || !strings.Contains(serialized, "; HttpOnly") || !strings.Contains(serialized, "; Partitioned") {
		t.Fatalf("missing security attributes: %q", serialized)
	}

	invalid := []*fh.Cookie{
		{Name: "bad\r\nInjected", Value: "x", Secure: true},
		{Name: "session", Value: "x; injected", Secure: true},
		{Name: "__Host-session", Value: "x", Path: "/", Secure: false},
		{Name: "session", Value: "x", SameSite: fh.SameSiteNone, Secure: false},
		{Name: "session", Value: "x", Domain: "evil; Domain=example.com", Secure: true},
	}
	for _, cookie := range invalid {
		if cookie.Valid() == nil || cookie.String() != "" {
			t.Fatalf("accepted invalid cookie: %#v", cookie)
		}
	}

	deletion := (&fh.Cookie{Name: "session", Value: "", Path: "/", MaxAge: -1, Expires: time.Unix(0, 0), Secure: true}).String()
	if !strings.Contains(deletion, "Max-Age=0") || !strings.Contains(deletion, "GMT") {
		t.Fatalf("invalid deletion cookie: %q", deletion)
	}
}

func TestParseCookieUsesFirstDuplicate(t *testing.T) {
	cookies := fh.ParseCookie("sid=trusted; sid=shadow; quoted=\"value\"")
	if cookies["sid"] != "trusted" || cookies["quoted"] != "value" {
		t.Fatalf("unexpected cookies: %#v", cookies)
	}
}

func TestSessionTokenRotationTamperingAndBinding(t *testing.T) {
	oldKey := []byte("old-key-32-bytes-long-for-testing!!")
	newKey := []byte("new-key-32-bytes-long-for-testing!!")
	store := kv.NewMemoryStore(kv.WithShardCount(1), kv.WithMaxEntries(defaultMaxSessions))
	oldManager := NewSessionManager(store, SessionCookieName("sid"), SessionSecrets(oldKey))
	s := oldManager.NewSession()
	s.Set("user", "alice")
	response := newTestCookieCtx()
	if err := oldManager.Save(response, s); err != nil {
		t.Fatal(err)
	}
	oldToken := response.FirstCookie()

	rotated := NewSessionManager(store, SessionCookieName("sid"), SessionSecrets(newKey, oldKey))
	request := newTestCookieCtx()
	setRequestCookie(request, "sid", oldToken)
	loaded, err := rotated.Load(request)
	if err != nil || loaded.Get("user") != "alice" {
		t.Fatalf("rotation load=%#v err=%v", loaded, err)
	}
	newResponse := newTestCookieCtx()
	if err := rotated.Save(newResponse, loaded); err != nil {
		t.Fatal(err)
	}
	if newResponse.FirstCookie() == oldToken {
		t.Fatal("session token was not signed with the active key")
	}

	replacementChar := "A"
	if oldToken[len(oldToken)-1] == 'A' {
		replacementChar = "B"
	}
	tampered := oldToken[:len(oldToken)-1] + replacementChar
	badRequest := newTestCookieCtx()
	setRequestCookie(badRequest, "sid", tampered)
	replacement, err := rotated.Load(badRequest)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.ID == s.ID {
		t.Fatal("tampered token loaded the stored session")
	}

	otherName := NewSessionManager(store, SessionCookieName("other_sid"), SessionSecrets(oldKey))
	boundRequest := newTestCookieCtx()
	setRequestCookie(boundRequest, "other_sid", oldToken)
	bound, err := otherName.Load(boundRequest)
	if err != nil || bound.ID == s.ID {
		t.Fatal("token was not bound to its cookie name")
	}
}

func TestInvalidSessionCookieCannotBlockValidSessionShard(t *testing.T) {
	store := kv.NewMemoryStore(kv.WithShardCount(1), kv.WithMaxEntries(defaultMaxSessions))
	manager := NewSessionManager(store,
		SessionSecrets([]byte("test-secret-must-be-32-bytes-ok!!")),
		SessionLockTimeout(10*time.Millisecond),
	)
	ctx := newTestCookieCtx()
	setRequestCookie(ctx, manager.CookieName(), "attacker-controlled-invalid-token")
	hash := sha256.Sum256([]byte("attacker-controlled-invalid-token"))
	shard := int(hash[0]) % len(manager.locks)
	manager.locks[shard].Lock()
	defer manager.locks[shard].Unlock()
	if _, _, err := manager.Begin(ctx); err != nil {
		t.Fatalf("invalid cookie waited on session shard lock: %v", err)
	}
}

func TestSessionLockWaitStopsWhenRequestIsCanceled(t *testing.T) {
	store := kv.NewMemoryStore(kv.WithShardCount(1), kv.WithMaxEntries(defaultMaxSessions))
	manager := NewSessionManager(store, SessionSecrets([]byte("test-secret-must-be-32-bytes-ok!!")))
	session := manager.NewSession()
	response := newTestCookieCtx()
	if err := manager.Save(response, session); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte(session.ID))
	shard := int(hash[0]) % len(manager.locks)
	manager.locks[shard].Lock()
	defer manager.locks[shard].Unlock()

	request := newTestCookieCtx()
	setRequestCookie(request, manager.CookieName(), response.FirstCookie())
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	request.SetContext(requestCtx)
	if _, _, err := manager.Begin(request); !errors.Is(err, context.Canceled) {
		t.Fatalf("Begin error = %v, want context.Canceled", err)
	}
}

type panicSetStore struct{ kv.Store }

func (panicSetStore) Set(string, []byte, time.Duration) error { panic("store panic") }

func TestSessionCompletionUnlocksWhenStorePanics(t *testing.T) {
	baseStore := kv.NewMemoryStore(kv.WithShardCount(1), kv.WithMaxEntries(defaultMaxSessions))
	secret := []byte("test-secret-must-be-32-bytes-ok!!")
	seedManager := NewSessionManager(baseStore, SessionSecrets(secret))
	session := seedManager.NewSession()
	response := newTestCookieCtx()
	if err := seedManager.Save(response, session); err != nil {
		t.Fatal(err)
	}
	manager := NewSessionManager(panicSetStore{Store: baseStore}, SessionSecrets(secret))
	request := newTestCookieCtx()
	setRequestCookie(request, manager.CookieName(), response.FirstCookie())
	loaded, complete, err := manager.Begin(request)
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected store panic")
			}
		}()
		_ = complete(newTestCookieCtx())
	}()
	hash := sha256.Sum256([]byte(loaded.ID))
	shard := int(hash[0]) % len(manager.locks)
	if !manager.locks[shard].TryLock() {
		t.Fatal("session shard lock remained held after store panic")
	}
	manager.locks[shard].Unlock()
}

func TestSessionRegenerateDestroyAndStoreSnapshots(t *testing.T) {
	store := kv.NewMemoryStore(kv.WithShardCount(1), kv.WithMaxEntries(defaultMaxSessions))
	manager := NewSessionManager(store, SessionSecrets([]byte("0123456789abcdef0123456789abcdef")))
	s := manager.NewSession()
	s.Set("role", "admin")
	ctx := newTestCookieCtx()
	if err := manager.Save(ctx, s); err != nil {
		t.Fatal(err)
	}
	oldID := s.ID
	if err := manager.Regenerate(ctx, s); err != nil {
		t.Fatal(err)
	}
	if s.ID == oldID {
		t.Fatal("session ID did not rotate")
	}
	if old, _ := GetSession(store, oldID); old != nil {
		t.Fatal("old session survived regeneration")
	}

	stored, _ := GetSession(store, s.ID)
	s.Set("role", "changed-after-save")
	if stored.Get("role") != "admin" {
		t.Fatal("memory store leaked a mutable session pointer")
	}

	if err := manager.Destroy(ctx, s); err != nil {
		t.Fatal(err)
	}
	if err := manager.Save(ctx, s); err != nil {
		t.Fatal(err)
	}
	if resurrected, _ := GetSession(store, s.ID); resurrected != nil {
		t.Fatal("destroyed session was recreated")
	}
}

func TestFileStoreAtomicRoundTripAndValidation(t *testing.T) {
	dir := t.TempDir()
	store, err := kv.NewFileStore(dir)
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	s := &Session{ID: strings.Repeat("a", 64), Data: map[string]any{"user": "alice"}, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}
	if err := SetSession(store, s); err != nil {
		t.Fatal(err)
	}
	loaded, err := GetSession(store, s.ID)
	if err != nil || loaded.Get("user") != "alice" {
		t.Fatalf("loaded=%#v err=%v", loaded, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) == 0 {
		t.Fatalf("entries=%d err=%v", len(entries), err)
	}
	info, err := entries[0].Info()
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("mode=%v err=%v", info.Mode().Perm(), err)
	}
	if _, err := GetSession(store, "../../escape"); err == nil {
		t.Fatal("accepted unsafe session ID")
	}
}

func newTestCookieCtx() fh.Ctx {
	c := &fh.DefaultCtx{}
	c.RequestHeader().Init()
	return c
}

func setRequestCookie(c fh.Ctx, name, value string) {
	c.RequestHeader().Set("Cookie", name+"="+value)
}
