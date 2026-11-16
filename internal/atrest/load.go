package atrest

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
)

const maxKeyringFileSize = 64 << 10

// ReadKeyring loads root-owned or user-owner-only envelope keys from a local
// secret file. Never store this file next to a backup of the encrypted WAL.
func ReadKeyring(path string) (uint16, map[uint16][]byte, error) {
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return 0, nil, err
	}
	if !pathInfo.Mode().IsRegular() || pathInfo.Mode().Perm()&0077 != 0 {
		return 0, nil, errors.New("WAL key file must be regular and chmod 600")
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, nil, err
	}
	defer f.Close()
	openedInfo, err := f.Stat()
	if err != nil {
		return 0, nil, err
	}
	if !openedInfo.Mode().IsRegular() || openedInfo.Mode().Perm()&0077 != 0 || !os.SameFile(pathInfo, openedInfo) {
		return 0, nil, errors.New("WAL key file changed during open or has unsafe permissions")
	}
	if openedInfo.Size() < 0 || openedInfo.Size() > maxKeyringFileSize {
		return 0, nil, errors.New("WAL key file exceeds 64 KiB limit")
	}
	data, err := io.ReadAll(io.LimitReader(f, maxKeyringFileSize+1))
	if err != nil {
		clear(data)
		return 0, nil, err
	}
	defer clear(data)
	if len(data) > maxKeyringFileSize {
		return 0, nil, errors.New("WAL key file exceeds 64 KiB limit")
	}
	var raw struct {
		Active uint16                     `json:"active"`
		Keys   map[string]json.RawMessage `json:"keys"`
	}
	defer func() {
		for _, value := range raw.Keys {
			clear(value)
		}
	}()
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&raw); err != nil {
		return 0, nil, err
	}
	var extra any
	if err := d.Decode(&extra); !errors.Is(err, io.EOF) {
		return 0, nil, errors.New("invalid trailing data in WAL key file")
	}
	if len(raw.Keys) < 1 || len(raw.Keys) > 16 {
		return 0, nil, errors.New("invalid keyring size")
	}
	keys := make(map[uint16][]byte, len(raw.Keys))
	clearKeys := func() {
		for _, key := range keys {
			clear(key)
		}
	}
	for id, encoded := range raw.Keys {
		n, err := strconv.ParseUint(id, 10, 16)
		if err != nil {
			clearKeys()
			return 0, nil, err
		}
		if len(encoded) != 66 || encoded[0] != '"' || encoded[len(encoded)-1] != '"' {
			clearKeys()
			return 0, nil, errors.New("WAL keys must be random 32-byte hex strings")
		}
		b := make([]byte, 32)
		nDecoded, err := hex.Decode(b, encoded[1:len(encoded)-1])
		if err != nil || nDecoded != len(b) {
			clear(b)
			clearKeys()
			return 0, nil, errors.New("WAL keys must be random 32-byte hex strings")
		}
		keyID := uint16(n)
		if _, exists := keys[keyID]; exists {
			clear(b)
			clearKeys()
			return 0, nil, errors.New("duplicate numeric WAL key ID")
		}
		keys[keyID] = b
	}
	if _, ok := keys[raw.Active]; !ok {
		clearKeys()
		return 0, nil, errors.New("active WAL encryption key missing")
	}
	return raw.Active, keys, nil
}
