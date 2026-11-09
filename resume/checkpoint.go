package resume

// Crash-safe local replay progress. This is a persistence helper for the
// durable APPLICATION replay protocol; it never resurrects old packet
// numbers, ciphertext, transport channels, or expired authentication state.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	"github.com/oarkflow/fh/journal"
)

var ErrCheckpoint = errors.New("resume: invalid durable cursor checkpoint")

const checkpointMagic = "MIRACUR1"

// CursorFile records the last applied application event cursor. One process
// must be the sole writer to a given file (lock it at application level).
// The callback's effects MUST be idempotent because crashes can occur after
// committing the effect but before storing this checkpoint.
type CursorFile struct {
	mu      sync.Mutex
	path    string
	current uint64
}

func OpenCursorFile(path string) (*CursorFile, error) {
	if path == "" || !filepath.IsAbs(path) {
		return nil, ErrCheckpoint
	}
	c := &CursorFile{path: path}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return c, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, ErrCheckpoint
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(b) != 20 || string(b[:8]) != checkpointMagic || crc32.ChecksumIEEE(b[:16]) != binary.BigEndian.Uint32(b[16:]) {
		return nil, ErrCheckpoint
	}
	c.current = binary.BigEndian.Uint64(b[8:16])
	return c, nil
}
func (c *CursorFile) Cursor() uint64 { c.mu.Lock(); defer c.mu.Unlock(); return c.current }
func (c *CursorFile) Store(next uint64) error {
	if c == nil {
		return ErrCheckpoint
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if next < c.current {
		return ErrCheckpoint
	}
	if next == c.current {
		return nil
	}
	var b [20]byte
	copy(b[:8], checkpointMagic)
	binary.BigEndian.PutUint64(b[8:16], next)
	binary.BigEndian.PutUint32(b[16:], crc32.ChecksumIEEE(b[:16]))
	dir := filepath.Dir(c.path)
	tmp, err := os.CreateTemp(dir, ".mira-cursor-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err = tmp.Chmod(0600); err != nil {
		tmp.Close()
		return err
	}
	if _, err = tmp.Write(b[:]); err != nil {
		tmp.Close()
		return err
	}
	if err = tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, c.path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if closeErr := d.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("resume: directory sync after rename: %w", err)
	}
	c.current = next
	return nil
}

// ReplayCheckpointed applies the durable journal after its last fsynced local
// cursor. Each event advances the file only AFTER the callback succeeded;
// server-side ACK is sent by ReplayAfter after the replay completes.
func (c *Client) ReplayCheckpointed(ctx context.Context, cp *CursorFile, apply func(context.Context, journal.Event) error) (uint64, error) {
	if cp == nil || apply == nil {
		return 0, ErrCheckpoint
	}
	return c.ReplayAfter(ctx, cp.Cursor(), func(ctx context.Context, e journal.Event) error {
		if err := apply(ctx, e); err != nil {
			return err
		}
		return cp.Store(e.Cursor)
	})
}
