package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/oarkflow/fh/mw/deviceauth"
)

type clientState struct {
	UserID       string `json:"user_id"`
	DeviceID     string `json:"device_id"`
	PrivateKey   string `json:"private_key"`
	RecoveryCode string `json:"recovery_code,omitempty"`
}

func client(args []string) {
	flags := flag.NewFlagSet("client", flag.ExitOnError)
	baseURL := flags.String("url", "http://127.0.0.1:8093", "demo server URL")
	action := flags.String("action", "demo", "register, login, recover, change, or demo")
	userID := flags.String("user", "", "account ID")
	inviteCode := flags.String("invite-code", os.Getenv("FH_DEVICEAUTH_INVITE_CODE"), "one-time invite issued for this user")
	recoveryCode := flags.String("recovery-code", "", "one-time recovery code")
	statePath := flags.String("state", "deviceauth-client.json", "local private-key state file; keep private")
	_ = flags.Parse(args)
	switch *action {
	case "register", "login", "recover", "change", "demo":
	default:
		log.Fatal("-action must be register, login, recover, change, or demo")
	}
	httpClient := &http.Client{Timeout: 5 * time.Second}
	ctx := context.Background()
	if !validUserID(*userID) {
		log.Fatal("pass a valid -user account ID (3-128 letters, digits, . _ - @)")
	}
	state, loadErr := loadClientState(*statePath)
	hasState := loadErr == nil
	if loadErr != nil && !os.IsNotExist(loadErr) {
		log.Fatalf("read client state: %v", loadErr)
	}
	if hasState && state.UserID != *userID && *action != "recover" {
		log.Fatalf("state file belongs to %q, not %q", state.UserID, *userID)
	}

	switch *action {
	case "register":
		if hasState {
			log.Fatalf("state already exists at %s", *statePath)
		}
		state = register(ctx, httpClient, *baseURL, *userID, *inviteCode)
		if err := saveClientState(*statePath, state); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Registered %s. Save this recovery code somewhere safe: %s\nDevice state: %s\n", state.UserID, state.RecoveryCode, *statePath)
		return
	case "demo":
		if !hasState {
			state = register(ctx, httpClient, *baseURL, *userID, *inviteCode)
			if err := saveClientState(*statePath, state); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("Registered %s. Save recovery code: %s\n", state.UserID, state.RecoveryCode)
		}
	case "login":
		if !hasState {
			log.Fatalf("no device state at %s; register first", *statePath)
		}
	case "recover":
		if strings.TrimSpace(*recoveryCode) == "" {
			log.Fatal("recovery requires -recovery-code")
		}
		state = recoverAccount(ctx, httpClient, *baseURL, *userID, *recoveryCode)
		if err := saveClientState(*statePath, state); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Recovered %s. New recovery code: %s\n", state.UserID, state.RecoveryCode)
	case "change":
		if !hasState {
			log.Fatalf("no device state at %s; register first", *statePath)
		}
		state = changeDevice(ctx, httpClient, *baseURL, state)
		if err := saveClientState(*statePath, state); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Changed device for %s\n", state.UserID)
	}

	session := login(ctx, httpClient, *baseURL, state)
	response := getProtected(ctx, httpClient, *baseURL, state, session)
	fmt.Printf("Authenticated response: %s\n", bytes.TrimSpace(response))
}

func register(ctx context.Context, client *http.Client, baseURL, userID, invite string) clientState {
	if invite == "" {
		log.Fatal("registration requires -invite-code issued for this user")
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		log.Fatal(err)
	}
	var start struct {
		UserID      string `json:"user_id"`
		DeviceID    string `json:"device_id"`
		ChallengeID string `json:"challenge_id"`
		Challenge   string `json:"challenge"`
	}
	requestJSON(ctx, client, http.MethodPost, baseURL+"/auth/register/begin", map[string]string{"user_id": userID, "invite_code": invite}, &start, nil, "", nil)
	signature, err := deviceauth.SignChallenge(start.Challenge, privateKey)
	if err != nil {
		log.Fatal(err)
	}
	var done struct {
		RecoveryCode string `json:"recovery_code"`
	}
	requestJSON(ctx, client, http.MethodPost, baseURL+"/auth/register/finish", map[string]string{"challenge_id": start.ChallengeID, "signature": signature, "public_key": base64.RawURLEncoding.EncodeToString(publicKey)}, &done, nil, "", nil)
	return clientState{UserID: userID, DeviceID: start.DeviceID, PrivateKey: base64.RawURLEncoding.EncodeToString(privateKey), RecoveryCode: done.RecoveryCode}
}

func login(ctx context.Context, client *http.Client, baseURL string, state clientState) string {
	privateKey := decodePrivateKey(state.PrivateKey)
	var start struct {
		ChallengeID string `json:"challenge_id"`
		Challenge   string `json:"challenge"`
	}
	requestJSON(ctx, client, http.MethodPost, baseURL+"/auth/login/begin", map[string]string{"user_id": state.UserID, "device_id": state.DeviceID}, &start, nil, "", nil)
	signature, err := deviceauth.SignChallenge(start.Challenge, privateKey)
	if err != nil {
		log.Fatal(err)
	}
	var result struct {
		Session string `json:"session"`
	}
	requestJSON(ctx, client, http.MethodPost, baseURL+"/auth/login/finish", map[string]string{"challenge_id": start.ChallengeID, "signature": signature}, &result, nil, "", nil)
	return result.Session
}

func recoverAccount(ctx context.Context, client *http.Client, baseURL, userID, recoveryCode string) clientState {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		log.Fatal(err)
	}
	var start struct {
		DeviceID    string `json:"device_id"`
		ChallengeID string `json:"challenge_id"`
		Challenge   string `json:"challenge"`
	}
	requestJSON(ctx, client, http.MethodPost, baseURL+"/auth/recovery/begin", map[string]string{"user_id": userID, "recovery_code": recoveryCode}, &start, nil, "", nil)
	signature, err := deviceauth.SignChallenge(start.Challenge, privateKey)
	if err != nil {
		log.Fatal(err)
	}
	var result struct {
		RecoveryCode string `json:"recovery_code"`
	}
	requestJSON(ctx, client, http.MethodPost, baseURL+"/auth/recovery/finish", map[string]string{"challenge_id": start.ChallengeID, "signature": signature, "public_key": base64.RawURLEncoding.EncodeToString(publicKey)}, &result, nil, "", nil)
	return clientState{UserID: userID, DeviceID: start.DeviceID, PrivateKey: base64.RawURLEncoding.EncodeToString(privateKey), RecoveryCode: result.RecoveryCode}
}

func changeDevice(ctx context.Context, client *http.Client, baseURL string, state clientState) clientState {
	oldKey := decodePrivateKey(state.PrivateKey)
	session := login(ctx, client, baseURL, state)
	var start struct {
		DeviceID    string `json:"device_id"`
		ChallengeID string `json:"challenge_id"`
		Challenge   string `json:"challenge"`
	}
	requestJSON(ctx, client, http.MethodPost, baseURL+"/auth/change/begin", struct{}{}, &start, nil, session, oldKey)
	newPublic, newPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		log.Fatal(err)
	}
	signature, err := deviceauth.SignChallenge(start.Challenge, newPrivate)
	if err != nil {
		log.Fatal(err)
	}
	body := map[string]string{"challenge_id": start.ChallengeID, "signature": signature, "public_key": base64.RawURLEncoding.EncodeToString(newPublic)}
	requestJSON(ctx, client, http.MethodPost, baseURL+"/auth/change/finish", body, nil, nil, session, oldKey)
	state.DeviceID = start.DeviceID
	state.PrivateKey = base64.RawURLEncoding.EncodeToString(newPrivate)
	return state
}

func getProtected(ctx context.Context, client *http.Client, baseURL string, state clientState, session string) []byte {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/api/me", nil)
	if err != nil {
		log.Fatal(err)
	}
	privateKey := decodePrivateKey(state.PrivateKey)
	if err := deviceauth.SignRequest(req, nil, session, privateKey, time.Now(), 1<<20); err != nil {
		log.Fatal(err)
	}
	response, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		log.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		log.Fatalf("protected request failed (%s): %s", response.Status, body)
	}
	return body
}

func requestJSON(ctx context.Context, client *http.Client, method, url string, input, output any, headers map[string]string, session string, key ed25519.PrivateKey) {
	var data []byte
	var err error
	if input != nil {
		data, err = json.Marshal(input)
		if err != nil {
			log.Fatal(err)
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(data))
	if err != nil {
		log.Fatal(err)
	}
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	if session != "" {
		if err := deviceauth.SignRequest(req, data, session, key, time.Now(), 1<<20); err != nil {
			log.Fatal(err)
		}
	}
	response, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		log.Fatal(err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		log.Fatalf("%s %s: %s: %s", method, url, response.Status, body)
	}
	if output != nil {
		if err := json.Unmarshal(body, output); err != nil {
			log.Fatalf("decode response: %v", err)
		}
	}
}

func loadClientState(path string) (clientState, error) {
	info, err := os.Stat(path)
	if err != nil {
		return clientState{}, err
	}
	if info.Mode().Perm()&0077 != 0 {
		return clientState{}, errors.New("state file permissions must be 0600")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return clientState{}, err
	}
	var state clientState
	if err := json.Unmarshal(data, &state); err != nil || state.UserID == "" || state.DeviceID == "" || state.PrivateKey == "" {
		return clientState{}, errors.New("invalid device state file")
	}
	return state, nil
}

func saveClientState(path string, state clientState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		return err
	}
	return os.Chmod(path, 0600)
}

func decodePrivateKey(encoded string) ed25519.PrivateKey {
	value, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(value) != ed25519.PrivateKeySize {
		log.Fatal("invalid private key in local state")
	}
	return ed25519.PrivateKey(value)
}
