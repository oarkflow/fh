package session

import (
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
