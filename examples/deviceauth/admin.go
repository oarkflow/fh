package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

func admin(args []string) {
	flags := flag.NewFlagSet("admin invite", flag.ExitOnError)
	baseURL := flags.String("url", "http://127.0.0.1:8093", "demo server URL")
	userID := flags.String("user", "", "account ID to issue an invite for")
	_ = flags.Parse(args)
	token := os.Getenv("FH_DEVICEAUTH_ADMIN_TOKEN")
	if token == "" {
		log.Fatal("FH_DEVICEAUTH_ADMIN_TOKEN is required for admin invite")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	headers := map[string]string{"Authorization": "Bearer " + token}
	if !validUserID(*userID) {
		log.Fatal("pass a valid -user account ID (3-128 letters, digits, . _ - @)")
	}
	var result struct {
		UserID    string `json:"user_id"`
		Code      string `json:"invite_code"`
		ExpiresAt string `json:"expires_at"`
	}
	requestJSON(context.Background(), client, http.MethodPost, *baseURL+"/admin/invites", map[string]string{"user_id": *userID}, &result, headers, "", nil)
	fmt.Printf("Invite for %s (expires %s): %s\n", result.UserID, result.ExpiresAt, result.Code)
}
