package access

// Strict per-device certificate identity allowlist loader. A fresh immutable
// map is atomically swapped by the caller after validation; partial reads do
// not affect the currently active policy.

import (
	"bufio"
	"errors"
	"os"
	"strings"
)

func Parse(raw string) (map[string]struct{}, error) {
	result := make(map[string]struct{})
	scanner := bufio.NewScanner(strings.NewReader(strings.ReplaceAll(raw, ",", "\n")))
	scanner.Buffer(make([]byte, 1024), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if len(line) > 256 || strings.ContainsAny(line, " \t\r\n\x00") {
			return nil, errors.New("access: invalid identity")
		}
		result[line] = struct{}{}
		if len(result) > 100000 {
			return nil, errors.New("access: allowlist exceeds limit")
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, errors.New("access: empty allowlist is not accepted")
	}
	return result, nil
}
func Load(file, inline string) (map[string]struct{}, error) {
	if file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		if len(b) > 1<<20 {
			return nil, errors.New("access: allowlist too large")
		}
		return Parse(string(b))
	}
	return Parse(inline)
}
