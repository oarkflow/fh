//go:build windows

package loadtest

// currentNoFileLimit has no direct RLIMIT_NOFILE equivalent on Windows (the
// handle ceiling is process-wide and much higher by default). Return a
// conservative default so descriptor-scaling tests still pick a small,
// CI-safe connection count on this platform.
func currentNoFileLimit() (uint64, error) {
	return 4096, nil
}
