package main

import (
	"encoding/base64"
	"net/url"
	"testing"
)

func TestRegistrationGrantIsOneShotAndSessionBound(t *testing.T) {
	store := newGrantStore()
	token, err := store.issue("user-123", "session-a")
	if err != nil {
		t.Fatal(err)
	}
	if store.consume(token, "user-123", "session-b") {
		t.Fatal("grant accepted for a different web session")
	}
	if store.consume(token, "user-123", "session-a") {
		t.Fatal("failed binding attempt did not burn the one-time grant")
	}

	token, err = store.issue("user-123", "session-a")
	if err != nil {
		t.Fatal(err)
	}
	if !store.consume(token, "user-123", "session-a") {
		t.Fatal("valid registration grant was rejected")
	}
	if store.consume(token, "user-123", "session-a") {
		t.Fatal("registration grant was accepted more than once")
	}
}

func TestRegistrationGrantRejectsMalformedToken(t *testing.T) {
	store := newGrantStore()
	for _, token := range []string{"", "not-base64!", base64.RawURLEncoding.EncodeToString(make([]byte, 31))} {
		if store.consume(token, "user-123", "session-a") {
			t.Fatalf("malformed registration token %q was accepted", token)
		}
	}
}

func TestDevelopmentOriginPolicy(t *testing.T) {
	origin, err := url.Parse("http://127.0.0.1:8080")
	if err != nil {
		t.Fatal(err)
	}
	allowed := exampleOrigins(origin)
	for _, host := range []string{"localhost:8080", "127.0.0.1:8080", "0.0.0.0:8080"} {
		selected, ok := originForHost(allowed, host)
		if !ok || selected != "http://"+host {
			t.Fatalf("host %q selected origin=%q ok=%v", host, selected, ok)
		}
	}
	if _, ok := originForHost(allowed, "example.com:8080"); ok {
		t.Fatal("unlisted host was accepted")
	}

	production, _ := url.Parse("https://app.example.com")
	if origins := exampleOrigins(production); len(origins) != 1 || origins[0] != production.String() {
		t.Fatalf("production origin aliases=%v", origins)
	}
}
