package atrest

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strconv"
)

// ReadKeyring loads root-owned or user-owner-only envelope keys from a local
// secret file. Never store this file next to a backup of the encrypted WAL.
func ReadKeyring(path string) (uint16, map[uint16][]byte, error) {
	st, err := os.Stat(path)
	if err != nil {
		return 0, nil, err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		return 0, nil, errors.New("WAL key file must be regular and chmod 600")
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, nil, err
	}
	defer f.Close()
	var raw struct {
		Active uint16            `json:"active"`
		Keys   map[string]string `json:"keys"`
	}
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if err := d.Decode(&raw); err != nil {
		return 0, nil, err
	}
	if len(raw.Keys) < 1 || len(raw.Keys) > 16 {
		return 0, nil, errors.New("invalid keyring size")
	}
	keys := make(map[uint16][]byte, len(raw.Keys))
	for id, encoded := range raw.Keys {
		n, err := strconv.ParseUint(id, 10, 16)
		if err != nil {
			return 0, nil, err
		}
		b, err := hex.DecodeString(encoded)
		if err != nil || len(b) != 32 {
			return 0, nil, errors.New("WAL keys must be random 32-byte hex strings")
		}
		keys[uint16(n)] = b
	}
	if _, ok := keys[raw.Active]; !ok {
		return 0, nil, errors.New("active WAL encryption key missing")
	}
	return raw.Active, keys, nil
}
