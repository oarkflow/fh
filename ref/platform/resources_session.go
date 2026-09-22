package platform

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/oarkflow/fh/mw/session"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

// Session providers.
//
// All three wrap fh's own session manager over a kv.Store, so the cookie
// handling, signing and rotation are the framework's rather than reimplemented
// here. The only difference between them is where session state lives:
//
//	session.memory — one replica, lost on restart. Development only.
//	session.file   — survives a restart, shared only within one filesystem.
//	session.sql    — shared across replicas, which is the only one of the three
//	                 that is correct behind a load balancer.
//
// The secret is validated at load time, not at first use. A 16-byte session
// secret is not a runtime warning to be noticed in a log; it is a reason to
// refuse to start.

func registerSessionResources(r *Registry) {
	mustResource(r, "session.memory", sessionFactory(sessionBackendMemory), ResourceKindInfo{
		Family:   "session",
		Summary:  "In-process sessions. Lost on restart and invisible to other replicas — development only.",
		Provides: []string{"Session"},
		Config:   sessionConfigFields(),
	})
	mustResource(r, "session.file", sessionFactory(sessionBackendFile), ResourceKindInfo{
		Family:   "session",
		Summary:  "On-disk sessions surviving a restart, shared only between processes on one filesystem.",
		Provides: []string{"Session"},
		Config:   append(sessionConfigFields(), ConfigField{Name: "dir", Type: "string", Required: true}),
	})
	mustResource(r, "session.sql", sessionFactory(sessionBackendSQL), ResourceKindInfo{
		Family:   "session",
		Summary:  "Sessions in a SQL table. Correct across replicas; the right choice behind a load balancer.",
		Provides: []string{"Session"},
		Config: append(sessionConfigFields(),
			ConfigField{Name: "database", Type: "resource", Required: true},
			ConfigField{Name: "table", Type: "string", Default: "platform_sessions"},
			ConfigField{Name: "migrate", Type: "bool", Default: "true"},
		),
	})
}

type sessionBackend int

const (
	sessionBackendMemory sessionBackend = iota
	sessionBackendFile
	sessionBackendSQL
)

func sessionConfigFields() []ConfigField {
	return []ConfigField{
		{Name: "secret", Type: "string", Required: true, Summary: "At least 32 bytes of signing key. Keep it in the environment."},
		{Name: "previous_secrets", Type: "[]string", Summary: "Older keys still accepted for verification, so a rotation does not log everybody out"},
		{Name: "cookie", Type: "string", Default: "sid", Summary: "Cookie name"},
		{Name: "max_age", Type: "duration", Default: "24h"},
		{Name: "secure", Type: "bool", Default: "false", Summary: "Require HTTPS. Set true in production."},
		{Name: "max_entries", Type: "int", Default: "100000", Summary: "Memory backend eviction bound"},
		{Name: "gc_interval", Type: "duration", Default: "1m"},
	}
}

// SessionManager is the handle a session resource exposes.
type SessionManager = session.SessionManager

func sessionFactory(backend sessionBackend) ResourceFactory {
	return ResourceFactoryFunc(func(ctx context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
		known := []string{"secret", "previous_secrets", "cookie", "max_age", "secure", "max_entries", "gc_interval"}
		switch backend {
		case sessionBackendFile:
			known = append(known, "dir")
		case sessionBackendSQL:
			known = append(known, "database", "table", "migrate")
		}
		if err := rejectUnknownConfig(spec.Kind, spec.Config, known...); err != nil {
			return nil, nil, err
		}

		secret, err := requiredString(spec.Config, "secret")
		if err != nil {
			return nil, nil, fmt.Errorf("resource %q: %w", spec.Name, err)
		}
		if len(secret) < 32 {
			return nil, nil, fmt.Errorf("resource %q: session secret must contain at least 32 bytes, got %d", spec.Name, len(secret))
		}
		secrets := [][]byte{[]byte(secret)}
		for _, previous := range configStrings(spec.Config, "previous_secrets") {
			if len(previous) < 32 {
				return nil, nil, fmt.Errorf("resource %q: every previous session secret must contain at least 32 bytes", spec.Name)
			}
			secrets = append(secrets, []byte(previous))
		}

		gc, err := configDuration(spec.Config, "gc_interval", time.Minute)
		if err != nil {
			return nil, nil, err
		}
		maxAge, err := configDuration(spec.Config, "max_age", 24*time.Hour)
		if err != nil {
			return nil, nil, err
		}

		var (
			store  kv.Store
			closer io.Closer
		)
		switch backend {
		case sessionBackendFile:
			dir, err := requiredString(spec.Config, "dir")
			if err != nil {
				return nil, nil, fmt.Errorf("resource %q: %w", spec.Name, err)
			}
			fileStore, err := kv.NewFileStore(dir, kv.WithFileGCInterval(gc))
			if err != nil {
				return nil, nil, err
			}
			store, closer = fileStore, fileStore
		case sessionBackendSQL:
			sqlSpec := spec
			if configString(sqlSpec.Config, "table", "") == "" {
				// Copy rather than mutate: the caller's spec is also what the
				// redacted Document reports, and silently rewriting it would
				// make introspection disagree with the file on disk.
				sqlSpec.Config = make(map[string]any, len(spec.Config)+1)
				for key, value := range spec.Config {
					sqlSpec.Config[key] = value
				}
				sqlSpec.Config["table"] = "platform_sessions"
			}
			sqlSpec.Config = filterKeys(sqlSpec.Config, "database", "table", "migrate", "gc_interval")
			sqlStore, sqlCloser, err := openSQLCache(ctx, sqlSpec)
			if err != nil {
				return nil, nil, err
			}
			typed, ok := sqlStore.(kv.Store)
			if !ok {
				return nil, nil, fmt.Errorf("resource %q: SQL session store does not satisfy the kv contract", spec.Name)
			}
			store, closer = typed, sqlCloser
		default:
			maxEntries, err := configInt(spec.Config, "max_entries", 100000)
			if err != nil {
				return nil, nil, err
			}
			memoryStore := kv.NewMemoryStore(kv.WithMaxEntries(maxEntries), kv.WithGCInterval(gc))
			store, closer = memoryStore, memoryStore
		}

		manager := session.NewSessionManager(store,
			session.SessionSecrets(secrets...),
			session.SessionCookieName(configString(spec.Config, "cookie", "sid")),
			session.SessionMaxAge(maxAge),
			session.SessionSecure(configBool(spec.Config, "secure", false)),
		)
		return manager, closer, nil
	})
}

// filterKeys returns a copy of config holding only the listed keys. It is how
// one provider delegates to another without passing along config the delegate
// would reject as unknown.
func filterKeys(config map[string]any, keys ...string) map[string]any {
	out := make(map[string]any, len(keys))
	for _, key := range keys {
		if value, ok := config[key]; ok {
			out[key] = value
		}
	}
	return out
}
