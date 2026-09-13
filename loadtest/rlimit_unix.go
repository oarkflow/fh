//go:build !windows

package loadtest

import "syscall"

// currentNoFileLimit returns the process's current RLIMIT_NOFILE soft limit,
// used to scale connection-count tests safely below the real OS ceiling
// instead of guessing a fixed number that might exhaust descriptors in CI.
func currentNoFileLimit() (uint64, error) {
	var rl syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl); err != nil {
		return 0, err
	}
	return uint64(rl.Cur), nil
}
