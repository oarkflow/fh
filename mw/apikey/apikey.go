package apikey

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

const (
	LocalKeyRecord = "api_key_record"
	LocalKeyID     = "api_key_id"
	registryKey    = "registry"
)

const maxRegistryFileSize = 32 << 20

type LookupFunc func(ctx fh.Ctx, key string) bool
type ErrorHandler func(ctx fh.Ctx) error

type KeyRecord struct {
	ID          string
	Name        string
	Hash        string
	Prefix      string
	TenantID    string
	SubjectID   string
	Scopes      []string
	Permissions []string
	ExpiresAt   time.Time
	Revoked     bool
	AllowedIPs  []string
}

type UsageHook func(ctx fh.Ctx, rec KeyRecord)

type Config struct {
	Header string
	Query  string
	Cookie string
	// Keys, Lookup, and HashedKeys authenticate a key with no revocation or
	// scope metadata attached (unlike Store, whose KeyRecord carries
	// Scopes/Revoked/ExpiresAt). A key accepted through these paths cannot
	// be revoked without redeploying the app, and if RequiredScopes is set,
	// these paths can never satisfy it (see RequiredScopes below) — use
	// Store for any key that needs revocation or scope enforcement.
	Keys       []string
	Lookup     LookupFunc
	HashedKeys map[string]string
	Store      kv.Store
	// RequiredScopes is enforced uniformly across every auth path. Keys
	// authenticated via Lookup/Keys/HashedKeys carry no scope metadata, so
	// they are rejected whenever RequiredScopes is non-empty rather than
	// silently bypassing the requirement.
	RequiredScopes []string
	SetPrincipal   bool
	OnUsage        UsageHook
	Next           func(fh.Ctx) bool
	Error          ErrorHandler
}

func New(config Config) fh.HandlerFunc {
	if config.Header == "" {
		config.Header = "X-API-Key"
	}
	allowed := make([][]byte, 0, len(config.Keys))
	for _, k := range config.Keys {
		if k != "" {
			allowed = append(allowed, []byte(k))
		}
	}
	if config.Error == nil {
		config.Error = func(c fh.Ctx) error {
			return fh.NewHTTPError(fh.StatusUnauthorized, "API_KEY_INVALID", "API key is missing or invalid")
		}
	}
	return func(c fh.Ctx) error {
		if config.Next != nil && config.Next(c) {
			return c.Next()
		}
		key := extractKey(c, config)
		if key == "" {
			return config.Error(c)
		}
		var rec KeyRecord
		var recordOK bool
		ok := false
		if config.Store != nil {
			id, _ := SplitKey(key)
			if id != "" {
				r, exists, err := Lookup(config.Store, id)
				if err != nil {
					return fh.InternalError(fmt.Errorf("api key store lookup: %w", err))
				}
				if exists && VerifyRecord(c, key, r) {
					rec, recordOK, ok = r, true, true
				}
			}
		}
		if !ok && len(config.HashedKeys) > 0 {
			id, _ := SplitKey(key)
			if hash, exists := config.HashedKeys[id]; exists && ConstantTimeHashEqual(key, hash) {
				rec, recordOK, ok = KeyRecord{ID: id, Hash: hash}, true, true
			}
		}
		if !ok && config.Lookup != nil {
			ok = config.Lookup(c, key)
		}
		if !ok {
			kb := []byte(key)
			for _, want := range allowed {
				if hmac.Equal(kb, want) {
					ok = true
					break
				}
			}
		}
		// Enforced uniformly regardless of which path authenticated the
		// key: Lookup/Keys/HashedKeys carry no scope metadata, so if the
		// app requires scopes, a key from those paths can never satisfy it
		// and must be rejected rather than silently bypassing the
		// requirement (previously only the Store path checked scopes).
		if ok && len(config.RequiredScopes) > 0 && !hasScopes(rec.Scopes, config.RequiredScopes) {
			ok = false
		}
		if !ok {
			return config.Error(c)
		}
		if recordOK {
			c.Locals(LocalKeyRecord, rec)
			c.Locals(LocalKeyID, rec.ID)
			if config.SetPrincipal && rec.SubjectID != "" {
				fh.SetPrincipal(c, fh.Principal{ID: rec.SubjectID, Subject: rec.SubjectID, TenantID: rec.TenantID, Scopes: rec.Scopes, Permissions: rec.Permissions, Type: "api_key", AuthMethod: "api_key"})
			}
			if config.OnUsage != nil {
				config.OnUsage(c, rec)
			}
		}
		return c.Next()
	}
}

func extractKey(c fh.Ctx, cfg Config) string {
	key := strings.TrimSpace(c.Get(cfg.Header))
	if strings.HasPrefix(strings.ToLower(key), "apikey ") {
		key = strings.TrimSpace(key[7:])
	}
	if key == "" && cfg.Query != "" {
		key = strings.TrimSpace(c.Query(cfg.Query))
	}
	if key == "" && cfg.Cookie != "" {
		key = strings.TrimSpace(c.GetCookie(cfg.Cookie))
	}
	return key
}
func VerifyRecord(c fh.Ctx, key string, rec KeyRecord) bool {
	if rec.Revoked || rec.Hash == "" || !ConstantTimeHashEqual(key, rec.Hash) {
		return false
	}
	if !rec.ExpiresAt.IsZero() && time.Now().After(rec.ExpiresAt) {
		return false
	}
	if rec.Prefix != "" && !strings.HasPrefix(key, rec.Prefix) {
		return false
	}
	if len(rec.AllowedIPs) > 0 {
		if c == nil || !ipAllowed(c.IP(), rec.AllowedIPs) {
			return false
		}
	}
	return true
}
func SplitKey(key string) (id, secret string) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", ""
	}
	if i := strings.IndexByte(key, '.'); i > 0 {
		return key[:i], key[i+1:]
	}
	if i := strings.LastIndexByte(key, '_'); i > 0 {
		return key[:i], key[i+1:]
	}
	return key, ""
}
func HashKey(key string) string { sum := sha256.Sum256([]byte(key)); return hex.EncodeToString(sum[:]) }
func ConstantTimeHashEqual(key, expectedHash string) bool {
	return hmac.Equal([]byte(HashKey(key)), []byte(strings.ToLower(strings.TrimSpace(expectedHash))))
}
func Generate(prefix string, secretBytes int) (key string, hash string, err error) {
	if secretBytes <= 0 {
		secretBytes = 32
	}
	b := make([]byte, secretBytes)
	if _, err = rand.Read(b); err != nil {
		return "", "", err
	}
	prefix = strings.TrimRight(prefix, "._-")
	if prefix == "" {
		prefix = "fh_key"
	}
	key = prefix + "." + base64.RawURLEncoding.EncodeToString(b)
	return key, HashKey(key), nil
}

func Lookup(store kv.Store, id string) (KeyRecord, bool, error) {
	data, ok, err := store.Get(registryKey)
	if err != nil || !ok {
		return KeyRecord{}, false, err
	}
	records, err := decodeRegistry(data)
	if err != nil {
		return KeyRecord{}, false, err
	}
	for _, rec := range records {
		if rec.ID == id {
			return rec, true, nil
		}
	}
	return KeyRecord{}, false, nil
}

func SaveRegistry(store kv.Store, records []KeyRecord) error {
	data, err := json.Marshal(records)
	if err != nil {
		return err
	}
	return store.Set(registryKey, data, 0)
}

func Set(store kv.Store, rec KeyRecord) error {
	if rec.ID == "" {
		return nil
	}
	records, err := loadRegistry(store)
	if err != nil {
		return err
	}
	for i := range records {
		if records[i].ID == rec.ID {
			records[i] = rec
			return SaveRegistry(store, records)
		}
	}
	records = append(records, rec)
	return SaveRegistry(store, records)
}

func Revoke(store kv.Store, id string) error {
	records, err := loadRegistry(store)
	if err != nil {
		return err
	}
	for i := range records {
		if records[i].ID == id {
			records[i].Revoked = true
			return SaveRegistry(store, records)
		}
	}
	return fmt.Errorf("api key not found")
}

func loadRegistry(store kv.Store) ([]KeyRecord, error) {
	data, ok, err := store.Get(registryKey)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return decodeRegistry(data)
}

func decodeRegistry(data []byte) ([]KeyRecord, error) {
	var records []KeyRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, err
	}
	return records, nil
}

func NewKVStore(records ...KeyRecord) (kv.Store, error) {
	store := kv.NewMemoryStore()
	if err := SaveRegistry(store, records); err != nil {
		return nil, err
	}
	return store, nil
}
func hasScopes(have, need []string) bool {
	if len(need) == 0 {
		return true
	}
	set := map[string]struct{}{}
	for _, s := range have {
		set[s] = struct{}{}
	}
	for _, s := range need {
		if _, ok := set[s]; !ok {
			return false
		}
	}
	return true
}
func ipAllowed(raw string, ranges []string) bool {
	ip := net.ParseIP(raw)
	if ip == nil {
		return false
	}
	for _, r := range ranges {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		if strings.Contains(r, "/") {
			_, n, err := net.ParseCIDR(r)
			if err == nil && n.Contains(ip) {
				return true
			}
			continue
		}
		if want := net.ParseIP(r); want != nil && want.Equal(ip) {
			return true
		}
	}
	return false
}
