//go:build js || wasip1

package kernel

import (
	"crypto/tls"
	"errors"
)

// Listen reports that inbound listeners are unavailable on this target.
func Listen(string, *tls.Config, KernelConfig, Host) error {
	return errors.New("fh: inbound TCP listeners are unavailable on js/wasm and wasip1")
}
