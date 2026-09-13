package config

import (
	"context"
	"fmt"
	"os"

	cfgenv "github.com/oarkflow/config/providers/env"
)

// loadDotEnv merges a local .env file into the process environment using
// oarkflow/config's env provider. Real environment variables always win: a
// value already set in the process is left untouched, so orchestrated
// deployments that inject secrets directly (Docker, systemd, Kubernetes) are
// unaffected whether or not a .env file happens to be present. A missing
// .env file is not an error — production is not expected to ship one.
//
// Separator is set to a byte that never appears in APP_* names so the
// provider returns each variable under its own literal key instead of
// folding underscores into a nested dot path.
func loadDotEnv(path string) error {
	provider := cfgenv.Provider{Path: path, Separator: "\x00"}
	values, _, err := provider.Load(context.Background())
	if err != nil {
		return fmt.Errorf("config: load %s: %w", path, err)
	}
	for key, value := range values {
		if _, ok := os.LookupEnv(key); ok {
			continue
		}
		text, ok := value.(string)
		if !ok {
			continue
		}
		if err := os.Setenv(key, text); err != nil {
			return fmt.Errorf("config: set %s from %s: %w", key, path, err)
		}
	}
	return nil
}
