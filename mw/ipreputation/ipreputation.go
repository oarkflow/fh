package ipreputation

import (
	"math"
	"net"
	"sync"
	"time"

	"github.com/oarkflow/fh"
)

type Verdict string

const (
	VerdictAllowed    Verdict = "allowed"
	VerdictSuspicious Verdict = "suspicious"
	VerdictBlocked    Verdict = "blocked"
)

type Score struct {
	Value     float64
	Reasons   []string
	UpdatedAt time.Time
}

type ReputationStore interface {
	Get(ip string) (*Score, bool)
	Set(ip string, score *Score)
	Update(ip string, delta float64, reason string)
}

type Config struct {
	Store               ReputationStore
	BlockThreshold      float64
	SuspiciousThreshold float64
	DecayRate           float64
	MaxEntries          int
	BlockDuration       time.Duration
	KeyFunc             func(fh.Ctx) string
	OnBlocked           func(fh.Ctx, *Score) error
	OnSuspicious        func(fh.Ctx, *Score) error
	Whitelist           []string
	Blacklist           []string
	TrustProxy          bool
	Skip                func(fh.Ctx) bool
}

func New(cfg Config) (fh.HandlerFunc, func()) {
	cfg = normalize(cfg)
	whitelist := make(map[string]bool, len(cfg.Whitelist))
	for _, ip := range cfg.Whitelist {
		whitelist[ip] = true
	}
	blacklist := make(map[string]bool, len(cfg.Blacklist))
	for _, ip := range cfg.Blacklist {
		blacklist[ip] = true
	}

	stop := make(chan struct{})
	if ms, ok := cfg.Store.(*MemoryStore); ok {
		go ms.startDecay(cfg.DecayRate, cfg.BlockDuration, stop)
	}

	handler := func(c fh.Ctx) error {
		if cfg.Skip != nil && cfg.Skip(c) {
			return c.Next()
		}

		key := cfg.KeyFunc(c)
		ip := extractIP(key)

		if whitelist[ip] {
			return c.Next()
		}

		if blacklist[ip] {
			return cfg.OnBlocked(c, &Score{Value: 100, Reasons: []string{"blacklisted"}})
		}

		score, exists := cfg.Store.Get(ip)
		if exists {
			switch {
			case score.Value >= cfg.BlockThreshold:
				return cfg.OnBlocked(c, score)
			case score.Value >= cfg.SuspiciousThreshold:
				if err := cfg.OnSuspicious(c, score); err != nil {
					return err
				}
			}
		}

		err := c.Next()

		status := c.StatusCode()
		recordScore(cfg.Store, ip, status)

		return err
	}

	shutdown := func() { close(stop) }
	return handler, shutdown
}

func normalize(cfg Config) Config {
	if cfg.BlockThreshold <= 0 {
		cfg.BlockThreshold = 80
	}
	if cfg.SuspiciousThreshold <= 0 {
		cfg.SuspiciousThreshold = 50
	}
	if cfg.DecayRate <= 0 {
		cfg.DecayRate = 0.95
	}
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = 100000
	}
	if cfg.BlockDuration <= 0 {
		cfg.BlockDuration = 30 * time.Minute
	}
	if cfg.Store == nil {
		cfg.Store = NewMemoryStore(cfg.MaxEntries)
	}
	if cfg.KeyFunc == nil {
		cfg.KeyFunc = func(c fh.Ctx) string {
			if cfg.TrustProxy {
				if xff := c.Get("X-Forwarded-For"); xff != "" {
					parts := splitComma(xff)
					for i := len(parts) - 1; i >= 0; i-- {
						if ip := net.ParseIP(parts[i]); ip != nil {
							return ip.String()
						}
					}
				}
				if xri := c.Get("X-Real-IP"); xri != "" {
					if ip := net.ParseIP(xri); ip != nil {
						return ip.String()
					}
				}
			}
			return c.IP()
		}
	}
	if cfg.OnBlocked == nil {
		cfg.OnBlocked = func(c fh.Ctx, _ *Score) error {
			return c.Status(fh.StatusForbidden).SendString("Forbidden")
		}
	}
	if cfg.OnSuspicious == nil {
		cfg.OnSuspicious = func(c fh.Ctx, _ *Score) error {
			return c.Next()
		}
	}
	return cfg
}

func extractIP(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

func splitComma(s string) []string {
	var parts []string
	for len(s) > 0 {
		idx := -1
		for i := 0; i < len(s); i++ {
			if s[i] == ',' {
				idx = i
				break
			}
		}
		if idx < 0 {
			parts = append(parts, s)
			break
		}
		parts = append(parts, s[:idx])
		s = s[idx+1:]
	}
	return parts
}

func recordScore(store ReputationStore, ip string, status int) {
	delta := 0.0
	reason := ""
	switch {
	case status == 401:
		delta = 5
		reason = "auth_failure"
	case status == 403:
		delta = 8
		reason = "forbidden"
	case status == 429:
		delta = 10
		reason = "rate_limited"
	case status == 400:
		delta = 3
		reason = "bad_request"
	case status >= 500:
		delta = 2
		reason = "server_error"
	case status == 200:
		delta = -1
		reason = "success"
	}

	if delta != 0 {
		store.Update(ip, delta, reason)
	}
}

type MemoryStore struct {
	mu      sync.RWMutex
	scores  map[string]*Score
	maxSize int
}

func NewMemoryStore(maxSize int) *MemoryStore {
	if maxSize <= 0 {
		maxSize = 100000
	}
	return &MemoryStore{
		scores:  make(map[string]*Score, maxSize/4),
		maxSize: maxSize,
	}
}

func (s *MemoryStore) Get(ip string) (*Score, bool) {
	s.mu.RLock()
	sc, ok := s.scores[ip]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return sc, true
}

func (s *MemoryStore) Set(ip string, score *Score) {
	s.mu.Lock()
	s.scores[ip] = score
	s.evict()
	s.mu.Unlock()
}

func (s *MemoryStore) Update(ip string, delta float64, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sc, ok := s.scores[ip]
	if !ok {
		sc = &Score{Value: 0}
		s.scores[ip] = sc
	}

	sc.Value = math.Max(0, sc.Value+delta)
	sc.Reasons = append(sc.Reasons, reason)
	if len(sc.Reasons) > 20 {
		sc.Reasons = sc.Reasons[len(sc.Reasons)-20:]
	}
	sc.UpdatedAt = time.Now()

	s.evict()
}

func (s *MemoryStore) evict() {
	if len(s.scores) <= s.maxSize {
		return
	}
	// Remove bottom 10% by score.
	type entry struct {
		ip    string
		value float64
	}
	entries := make([]entry, 0, len(s.scores)/2)
	for ip, sc := range s.scores {
		entries = append(entries, entry{ip, sc.Value})
	}
	// Partial sort: find the 10% threshold.
	threshold := len(entries) / 10
	if threshold < 1 {
		threshold = 1
	}
	for i := 0; i < threshold; i++ {
		minIdx := i
		for j := i + 1; j < len(entries); j++ {
			if entries[j].value < entries[minIdx].value {
				minIdx = j
			}
		}
		entries[i], entries[minIdx] = entries[minIdx], entries[i]
	}
	for i := 0; i < threshold; i++ {
		delete(s.scores, entries[i].ip)
	}
}

func (s *MemoryStore) startDecay(rate float64, blockDuration time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			now := time.Now()
			for ip, sc := range s.scores {
				if sc.Value <= 0 {
					delete(s.scores, ip)
					continue
				}
				if now.Sub(sc.UpdatedAt) > blockDuration && sc.Value >= 80 {
					continue
				}
				sc.Value *= rate
				if sc.Value < 0.1 {
					sc.Value = 0
				}
			}
			s.mu.Unlock()
		case <-stop:
			return
		}
	}
}
