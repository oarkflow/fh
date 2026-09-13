package kv

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

var (
	// ErrProviderClosed is returned when a namespace is requested after the
	// provider has been closed.
	ErrProviderClosed = errors.New("kv: provider is closed")
	// ErrInvalidNamespace is returned for an empty, malformed, or excessively
	// long state namespace.
	ErrInvalidNamespace = errors.New("kv: invalid namespace")
)

// Provider supplies isolated Store instances for application features.
//
// A namespace identifies one logical state domain, for example
// "sessions/default", "ratelimit/public-api", or "replay/webhooks". Calls for
// the same namespace must return the same logical store. Different namespaces
// must not observe each other's keys and Len results.
//
// The provider owns the stores it returns. Callers must close the Provider,
// rather than individual stores, after every feature using it has stopped.
// Implementations must be safe for concurrent use.
//
// Redis, PostgreSQL, and other remote adapters should implement Provider as
// the configuration and lifecycle boundary and return Store implementations
// whose atomic Mutate operation is atomic across all participating processes.
type Provider interface {
	Store(context.Context, string) (Store, error)
	Close() error
}

// MustStore obtains a namespaced store or panics. It is intended for
// application initialization where constructor failure is fatal. Libraries
// should call Provider.Store and return the error instead.
func MustStore(provider Provider, namespace string) Store {
	if provider == nil {
		panic("kv: nil provider")
	}
	store, err := provider.Store(context.Background(), namespace)
	if err != nil {
		panic(fmt.Errorf("kv: open namespace %q: %w", namespace, err))
	}
	if store == nil {
		panic(fmt.Sprintf("kv: provider returned nil store for namespace %q", namespace))
	}
	return store
}

// MemoryProvider supplies isolated in-process stores and owns their lifecycle.
// It is suitable for one process and for tests. It is not distributed or
// durable across restarts.
type MemoryProvider struct {
	mu     sync.Mutex
	stores map[string]Store
	opts   []MemoryOption
	closed bool
}

// NewMemoryProvider creates a provider that applies opts independently to
// every namespace it opens.
func NewMemoryProvider(opts ...MemoryOption) *MemoryProvider {
	return &MemoryProvider{
		stores: make(map[string]Store),
		opts:   append([]MemoryOption(nil), opts...),
	}
}

// Store returns the isolated in-memory store for namespace.
func (p *MemoryProvider) Store(ctx context.Context, namespace string) (Store, error) {
	if err := validateProviderRequest(ctx, namespace); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrProviderClosed
	}
	if store := p.stores[namespace]; store != nil {
		return store, nil
	}
	store := NewMemoryStore(p.opts...)
	p.stores[namespace] = store
	return store, nil
}

// Close closes every namespace. It is safe to call more than once.
func (p *MemoryProvider) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	stores := make([]Store, 0, len(p.stores))
	for _, store := range p.stores {
		stores = append(stores, store)
	}
	p.mu.Unlock()
	return closeStores(stores)
}

// FileProvider supplies one durable FileStore per namespace beneath a common
// root directory. Namespace directory names are SHA-256 hashes, so untrusted
// namespace text can never become a filesystem path.
type FileProvider struct {
	mu     sync.Mutex
	root   string
	stores map[string]Store
	opts   []FileOption
	closed bool
}

// NewFileProvider creates a durable shared-state provider rooted at root.
func NewFileProvider(root string, opts ...FileOption) (*FileProvider, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("kv: file provider root required")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return nil, err
	}
	return &FileProvider{
		root:   root,
		stores: make(map[string]Store),
		opts:   append([]FileOption(nil), opts...),
	}, nil
}

// Store returns the isolated durable store for namespace.
func (p *FileProvider) Store(ctx context.Context, namespace string) (Store, error) {
	if err := validateProviderRequest(ctx, namespace); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrProviderClosed
	}
	if store := p.stores[namespace]; store != nil {
		return store, nil
	}
	sum := sha256.Sum256([]byte(namespace))
	dir := filepath.Join(p.root, hex.EncodeToString(sum[:]))
	store, err := NewFileStore(dir, p.opts...)
	if err != nil {
		return nil, fmt.Errorf("kv: open namespace %q: %w", namespace, err)
	}
	p.stores[namespace] = store
	return store, nil
}

// Close closes every namespace. It is safe to call more than once.
func (p *FileProvider) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	stores := make([]Store, 0, len(p.stores))
	for _, store := range p.stores {
		stores = append(stores, store)
	}
	p.mu.Unlock()
	return closeStores(stores)
}

func validateProviderRequest(ctx context.Context, namespace string) error {
	if ctx == nil {
		return errors.New("kv: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if namespace == "" || len(namespace) > 128 || !utf8.ValidString(namespace) || strings.TrimSpace(namespace) != namespace {
		return ErrInvalidNamespace
	}
	for _, r := range namespace {
		if unicode.IsControl(r) {
			return ErrInvalidNamespace
		}
	}
	return nil
}

func closeStores(stores []Store) error {
	var errs []error
	for _, store := range stores {
		if store != nil {
			if err := store.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

var _ Provider = (*MemoryProvider)(nil)
var _ Provider = (*FileProvider)(nil)
