package ipwhitelist

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

var ErrForbidden = errors.New("ipwhitelist: forbidden")

type KeyFunc func(ctx fh.Ctx) string

type ForbiddenHandler func(ctx fh.Ctx) error

const listKey = "ipwhitelist:list"

type Config struct {
	Allowed []string
	Blocked []string

	Store      kv.Store
	BlockStore kv.Store

	KeyFunc KeyFunc

	Forbidden ForbiddenHandler
}

func New(allowed ...string) fh.HandlerFunc {
	return NewWithConfig(Config{
		Allowed: allowed,
	})
}

func NewWithConfig(config Config) fh.HandlerFunc {
	cfg, err := normalize(config)
	if err != nil {
		panic(err)
	}

	return func(ctx fh.Ctx) error {
		rawIP := ""

		if cfg.KeyFunc != nil {
			rawIP = cfg.KeyFunc(ctx)
		}

		if rawIP == "" {
			rawIP = ctx.IP()
		}

		ip := net.ParseIP(rawIP)
		if ip == nil {
			return cfg.Forbidden(ctx)
		}

		if cfg.BlockStore != nil && AllowedIP(cfg.BlockStore, ip) {
			return cfg.Forbidden(ctx)
		}

		if cfg.Store == nil || AllowedIP(cfg.Store, ip) {
			return ctx.Next()
		}

		return cfg.Forbidden(ctx)
	}
}

func normalize(cfg Config) (Config, error) {
	if cfg.Forbidden == nil {
		cfg.Forbidden = DefaultForbiddenHandler
	}
	if cfg.Store == nil && len(cfg.Allowed) > 0 {
		store := kv.NewMemoryStore()
		if err := SaveList(store, cfg.Allowed...); err != nil {
			return cfg, err
		}
		cfg.Store = store
	}
	if cfg.BlockStore == nil && len(cfg.Blocked) > 0 {
		store := kv.NewMemoryStore()
		if err := SaveList(store, cfg.Blocked...); err != nil {
			return cfg, err
		}
		cfg.BlockStore = store
	}
	return cfg, nil
}

func DefaultForbiddenHandler(ctx fh.Ctx) error {
	ctx.Set("Content-Type", "text/plain; charset=utf-8")
	return ctx.Status(403).SendString("Forbidden")
}

func SaveList(store kv.Store, allowed ...string) error {
	if _, _, err := parseEntries(allowed...); err != nil {
		return err
	}
	data, err := json.Marshal(allowed)
	if err != nil {
		return err
	}
	return store.Set(listKey, data, 0)
}

func LoadFile(store kv.Store, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("ipwhitelist: failed to read store file %q: %w", path, err)
	}
	var entries []string
	if err := json.Unmarshal(data, &entries); err != nil {
		return fmt.Errorf("ipwhitelist: failed to parse store file %q: %w", path, err)
	}
	if err := SaveList(store, entries...); err != nil {
		return fmt.Errorf("ipwhitelist: invalid entry in store file %q: %w", path, err)
	}
	return nil
}

func StartFileWatcher(store kv.Store, path string, interval time.Duration) (func(), error) {
	if err := LoadFile(store, path); err != nil {
		return nil, err
	}
	if interval <= 0 {
		return func() {}, nil
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = LoadFile(store, path)
			case <-stop:
				return
			}
		}
	}()
	var stopped atomic.Bool
	return func() {
		if !stopped.CompareAndSwap(false, true) {
			return
		}
		close(stop)
		<-done
	}, nil
}

func AllowedIP(store kv.Store, ip net.IP) bool {
	data, ok, err := store.Get(listKey)
	if err != nil || !ok {
		return false
	}
	var entries []string
	if err := json.Unmarshal(data, &entries); err != nil {
		return false
	}
	ips, networks, err := parseEntries(entries...)
	if err != nil {
		return false
	}
	for _, allowed := range ips {
		if allowed.Equal(ip) {
			return true
		}
	}
	for _, network := range networks {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

func parseEntries(allowed ...string) ([]net.IP, []*net.IPNet, error) {
	ips := make([]net.IP, 0, len(allowed))
	networks := make([]*net.IPNet, 0, len(allowed))

	for _, item := range allowed {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		if strings.Contains(item, "/") {
			ip, network, err := net.ParseCIDR(item)
			if err != nil {
				return nil, nil, err
			}
			network.IP = ip
			networks = append(networks, network)
			continue
		}

		ip := net.ParseIP(item)
		if ip == nil {
			return nil, nil, errors.New("ipwhitelist: invalid IP: " + item)
		}

		ips = append(ips, ip)
	}

	return ips, networks, nil
}
