//go:build !linux && !darwin && !freebsd

package durable

import (
	"errors"
	"os"
)

func lockExclusive(f *os.File) error {
	return errors.New("durable: exclusive file locking unavailable on this OS")
}
