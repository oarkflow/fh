package ipreputation

import (
	"encoding/json"
	"math"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/storage/kv"
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

type Config struct {
	Store               kv.Store
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
	index := newIndexState(cfg.MaxEntries)
	go startDecay(cfg.Store, index, cfg.DecayRate, cfg.BlockDuration, cfg.BlockThreshold, stop)

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

		score, exists := Get(cfg.Store, ip)
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
		recordScore(cfg.Store, index, ip, status)

		return err
	}

	var stopOnce sync.Once
	shutdown := func() { stopOnce.Do(func() { close(stop) }) }
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
		cfg.Store = kv.NewMemoryStore(kv.WithShardCount(1), kv.WithMaxEntries(cfg.MaxEntries))
	}
	if cfg.KeyFunc == nil {
		cfg.KeyFunc = func(c fh.Ctx) string { return c.IP() }
	}
	if cfg.OnBlocked == nil {
		cfg.OnBlocked = func(c fh.Ctx, _ *Score) error {
			return c.Status(fh.StatusForbidden).SendString("Forbidden")
		}
	}
	if cfg.OnSuspicious == nil {
		cfg.OnSuspicious = func(fh.Ctx, *Score) error { return nil }
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

func recordScore(store kv.Store, index *indexState, ip string, status int) {
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
		Update(store, index, ip, delta, reason)
	}
}

type indexState struct {
	mu    sync.Mutex
	score map[string]float64
	max   int
}

func newIndexState(max int) *indexState {
	if max <= 0 {
		max = 100000
	}
	return &indexState{score: make(map[string]float64, max/4), max: max}
}

func Get(store kv.Store, ip string) (*Score, bool) {
	data, ok, err := store.Get(ip)
	if err != nil || !ok {
		return nil, false
	}
	var sc Score
	if json.Unmarshal(data, &sc) != nil {
		return nil, false
	}
	return &sc, true
}

func Set(store kv.Store, index *indexState, ip string, score *Score) {
	if score == nil {
		return
	}
	clone := *score
	clone.Reasons = append([]string(nil), score.Reasons...)
	data, err := json.Marshal(&clone)
	if err != nil {
		return
	}
	if err := store.Set(ip, data, 0); err != nil {
		return
	}
	if index != nil {
		index.mu.Lock()
		index.score[ip] = clone.Value
		index.mu.Unlock()
		evict(store, index)
	}
}

func Update(store kv.Store, index *indexState, ip string, delta float64, reason string) {
	var newValue float64
	err := store.Mutate(ip, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		var sc Score
		if exists {
			if json.Unmarshal(current, &sc) != nil {
				sc = Score{}
			}
		}
		sc.Value = math.Max(0, sc.Value+delta)
		sc.Reasons = append(sc.Reasons, reason)
		if len(sc.Reasons) > 20 {
			sc.Reasons = sc.Reasons[len(sc.Reasons)-20:]
		}
		sc.UpdatedAt = time.Now()
		newValue = sc.Value
		data, err := json.Marshal(&sc)
		if err != nil {
			return nil, 0, false, err
		}
		return data, 0, true, nil
	})
	if err != nil {
		return
	}
	if index != nil {
		index.mu.Lock()
		index.score[ip] = newValue
		index.mu.Unlock()
		evict(store, index)
	}
}

// evict mirrors the original MemoryStore's policy: once over maxSize, sort
// tracked IPs by score value ascending and drop the lowest 10% (at least 1).
func evict(store kv.Store, index *indexState) {
	index.mu.Lock()
	if len(index.score) <= index.max {
		index.mu.Unlock()
		return
	}
	type entry struct {
		ip    string
		value float64
	}
	entries := make([]entry, 0, len(index.score))
	for ip, v := range index.score {
		entries = append(entries, entry{ip, v})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].value < entries[j].value
	})
	removeCount := len(entries) / 10
	if removeCount < 1 {
		removeCount = 1
	}
	victims := make([]string, 0, removeCount)
	for i := 0; i < removeCount && i < len(entries); i++ {
		victims = append(victims, entries[i].ip)
		delete(index.score, entries[i].ip)
	}
	index.mu.Unlock()
	for _, ip := range victims {
		_ = store.Delete(ip)
	}
}

func startDecay(store kv.Store, index *indexState, rate float64, blockDuration time.Duration, blockThreshold float64, stop <-chan struct{}) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			decayOnce(store, index, rate, blockDuration, blockThreshold)
		case <-stop:
			return
		}
	}
}

func decayOnce(store kv.Store, index *indexState, rate float64, blockDuration time.Duration, blockThreshold float64) {
	index.mu.Lock()
	ips := make([]string, 0, len(index.score))
	for ip := range index.score {
		ips = append(ips, ip)
	}
	index.mu.Unlock()

	now := time.Now()
	for _, ip := range ips {
		var removed, changed bool
		var newValue float64
		err := store.Mutate(ip, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
			if !exists {
				removed = true
				return nil, 0, false, nil
			}
			var sc Score
			if json.Unmarshal(current, &sc) != nil {
				removed = true
				return nil, 0, false, nil
			}
			if sc.Value <= 0 {
				removed = true
				return nil, 0, false, nil
			}
			if now.Sub(sc.UpdatedAt) > blockDuration && sc.Value >= blockThreshold {
				return nil, 0, false, nil
			}
			sc.Value *= rate
			if sc.Value < 0.1 {
				sc.Value = 0
			}
			newValue = sc.Value
			changed = true
			data, err := json.Marshal(&sc)
			if err != nil {
				return nil, 0, false, err
			}
			return data, 0, true, nil
		})
		if err != nil {
			continue
		}
		index.mu.Lock()
		if removed {
			delete(index.score, ip)
		} else if changed {
			index.score[ip] = newValue
		}
		index.mu.Unlock()
		if removed {
			_ = store.Delete(ip)
		}
	}
}
