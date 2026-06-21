package dagflow

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"time"
)

func signingSecret() []byte {
	s := os.Getenv("DAGFLOW_SIGNING_SECRET")
	if s == "" {
		s = "dev-change-me"
	}
	return []byte(s)
}
func SignResumeToken(taskID, workflowID, nodeID string, ttl time.Duration) string {
	exp := time.Now().Add(ttl).Unix()
	nonce := newID("nonce")
	payload := fmt.Sprintf("%s|%s|%s|%d|%s", taskID, workflowID, nodeID, exp, nonce)
	mac := hmac.New(sha256.New, signingSecret())
	mac.Write([]byte(payload))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return base64.RawURLEncoding.EncodeToString([]byte(payload + "|" + sig))
}
func VerifyResumeToken(token, taskID, nodeID string) error {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return err
	}
	parts := strings.Split(string(b), "|")
	if len(parts) != 6 {
		return fmt.Errorf("invalid token")
	}
	payload := strings.Join(parts[:5], "|")
	mac := hmac.New(sha256.New, signingSecret())
	mac.Write([]byte(payload))
	expect := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expect), []byte(parts[5])) {
		return fmt.Errorf("invalid token signature")
	}
	if parts[0] != taskID || parts[2] != nodeID {
		return fmt.Errorf("token does not match task/node")
	}
	var exp int64
	_, _ = fmt.Sscanf(parts[3], "%d", &exp)
	if time.Now().Unix() > exp {
		return fmt.Errorf("resume token expired")
	}
	return nil
}

func Redact(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, v := range x {
			lk := strings.ToLower(k)
			if strings.Contains(lk, "password") || strings.Contains(lk, "token") || strings.Contains(lk, "secret") || strings.Contains(lk, "api_key") || strings.Contains(lk, "authorization") || strings.Contains(lk, "cookie") {
				out[k] = "***REDACTED***"
			} else {
				out[k] = Redact(v)
			}
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, v := range x {
			out[i] = Redact(v)
		}
		return out
	default:
		return v
	}
}
