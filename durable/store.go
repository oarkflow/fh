// Package durable provides a crash-recoverable, single-writer append-only store
// for application messages and replay cursors. Transport ACKs are NOT durable
// receipts. Callers must authorize each record and use stable idempotency keys.
package durable

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/oarkflow/fh/protocol"
)

const maxRecord = 2 << 20

var table = crc32.MakeTable(crc32.Castagnoli)
var (
	ErrClosed   = errors.New("durable: closed")
	ErrConflict = errors.New("durable: idempotency key reused for different content")
	ErrCorrupt  = errors.New("durable: corrupt committed log entry")
	ErrLimit    = errors.New("durable: storage capacity reached")
)

type record struct {
	Type      string             `json:"type"`
	Principal protocol.Principal `json:"principal"`
	Cursor    uint64             `json:"cursor"`
	Message   protocol.Message   `json:"message,omitempty"`
	Key       string             `json:"key,omitempty"`
	Hash      [32]byte           `json:"hash,omitempty"`
}

type item struct {
	principal protocol.Principal
	message   protocol.Message
	key       string
}
type keyRecord struct {
	cursor uint64
	hash   [32]byte
}

// Store provides fsync-before-success, recovery of truncated trailing writes,
// idempotent append and identity-scoped cursors. One Store must exclusively own
// its file; do not open the same file from multiple processes concurrently.
// Its in-memory index is bounded by MaxLiveBytes; use per-tenant sharding for scale.
type Store struct {
	mu           sync.Mutex
	file         *os.File
	path         string
	closed       bool
	maxLiveBytes int64
	liveBytes    int64
	next         uint64
	items        map[uint64]item
	byPrincipal  map[protocol.Principal][]uint64
	acks         map[protocol.Principal]uint64
	keys         map[string]keyRecord
}

func Open(path string, maxLiveBytes int64) (*Store, error) {
	if maxLiveBytes <= 0 {
		maxLiveBytes = 64 << 20
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	if err := lockExclusive(f); err != nil {
		f.Close()
		return nil, fmt.Errorf("durable: exclusive file lock: %w", err)
	}
	st := &Store{file: f, path: path, maxLiveBytes: maxLiveBytes, items: make(map[uint64]item), byPrincipal: make(map[protocol.Principal][]uint64), acks: make(map[protocol.Principal]uint64), keys: make(map[string]keyRecord)}
	if err := st.recover(); err != nil {
		f.Close()
		return nil, err
	}
	if st.liveBytes > maxLiveBytes {
		f.Close()
		return nil, ErrLimit
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return st, nil
}

// Append performs a durable append without idempotency. Prefer AppendIdempotent
// when retries or reconnects can repeat an application operation.
func (s *Store) Append(ctx context.Context, p protocol.Principal, m protocol.Message) (uint64, error) {
	return s.AppendIdempotent(ctx, p, "", m)
}
func (s *Store) AppendIdempotent(ctx context.Context, p protocol.Principal, key string, m protocol.Message) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if p.Subject == "" {
		return 0, errors.New("durable: principal subject required")
	}
	if key != "" && len(key) > 256 {
		return 0, errors.New("durable: idempotency key too long")
	}
	if len(m.Payload) > 1<<20 {
		return 0, ErrLimit
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrClosed
	}
	// Hash includes principal and full message to reject accidental or malicious
	// reuse of an idempotency key for different commands.
	payload, err := json.Marshal(struct {
		P protocol.Principal
		M protocol.Message
	}{p, m})
	if err != nil {
		return 0, err
	}
	h := sum256(payload)
	index := indexKey(p, key)
	if key != "" {
		if v, ok := s.keys[index]; ok {
			if v.hash != h {
				return 0, ErrConflict
			}
			return v.cursor, nil
		}
	}
	if s.liveBytes+int64(len(m.Payload)) > s.maxLiveBytes || (key != "" && len(s.keys) >= 100000) {
		return 0, ErrLimit
	}
	cursor := s.next + 1
	r := record{Type: "append", Principal: p, Cursor: cursor, Message: clone(m), Key: key}
	if err := s.write(r); err != nil {
		return 0, err
	}
	s.next = cursor
	s.items[cursor] = item{p, clone(m), key}
	s.byPrincipal[p] = append(s.byPrincipal[p], cursor)
	s.liveBytes += int64(len(m.Payload))
	if key != "" {
		s.keys[index] = keyRecord{cursor, h}
	}
	return cursor, nil
}
func (s *Store) ReadAfter(ctx context.Context, p protocol.Principal, cursor uint64, max int) ([]protocol.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if max <= 0 || max > 1024 {
		return nil, errors.New("durable: max must be 1..1024")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	out := make([]protocol.Message, 0, min(max, 16))
	limit := maxUint(cursor, s.acks[p])
	ids := s.byPrincipal[p]
	offset := sort.Search(len(ids), func(i int) bool { return ids[i] > limit })
	for _, id := range ids[offset:] {
		if len(out) >= max {
			break
		}
		if it, ok := s.items[id]; ok {
			out = append(out, clone(it.message))
		}
	}
	return out, nil
}
func (s *Store) ReadRecordsAfter(ctx context.Context, p protocol.Principal, cursor uint64, max int) ([]Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if max <= 0 || max > 1024 {
		return nil, errors.New("durable: max must be 1..1024")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	out := make([]Entry, 0, min(max, 16))
	limit := maxUint(cursor, s.acks[p])
	ids := s.byPrincipal[p]
	offset := sort.Search(len(ids), func(i int) bool { return ids[i] > limit })
	for _, id := range ids[offset:] {
		if len(out) >= max {
			break
		}
		if it, ok := s.items[id]; ok {
			out = append(out, Entry{Cursor: id, Message: clone(it.message)})
		}
	}
	return out, nil
}

type Entry struct {
	Cursor  uint64
	Message protocol.Message
}

func (s *Store) Acknowledge(ctx context.Context, p protocol.Principal, cursor uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if cursor > s.next {
		return errors.New("durable: cursor beyond committed log")
	}
	if cursor <= s.acks[p] {
		return nil
	}
	if err := s.write(record{Type: "ack", Principal: p, Cursor: cursor}); err != nil {
		return err
	}
	s.acks[p] = cursor
	s.pruneIDs(p, cursor)
	for n, it := range s.items {
		if n <= cursor && it.principal == p {
			s.liveBytes -= int64(len(it.message.Payload))
			delete(s.items, n)
		}
	}
	return nil
}

// ReplayCursor is the last fsynced application acknowledgement for this identity.
func (s *Store) ReplayCursor(p protocol.Principal) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrClosed
	}
	return s.acks[p], nil
}

// Compact rewrites the fsynced live records, ACK cursors and idempotency
// tombstones to a new file then atomically replaces the old log. The cursor
// sequence is never renumbered, so reconnect replay remains deterministic.
func (s *Store) Compact(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	dir := filepath.Dir(s.path)
	f, err := os.CreateTemp(dir, ".mira-compact-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	_ = f.Chmod(0600)
	committed := false
	defer func() {
		if !committed {
			f.Close()
			os.Remove(tmp)
		}
	}()
	if err = lockExclusive(f); err != nil {
		return err
	}
	old := s.file
	s.file = f
	defer func() {
		if !committed {
			s.file = old
		}
	}()
	if err = s.writeUnflushed(record{Type: "checkpoint", Cursor: s.next}); err != nil {
		return err
	}
	ids := make([]uint64, 0, len(s.items))
	for id := range s.items {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if err = ctx.Err(); err != nil {
			return err
		}
		it := s.items[id]
		if err = s.writeUnflushed(record{Type: "restore", Cursor: id, Principal: it.principal, Message: it.message, Key: it.key}); err != nil {
			return err
		}
	}
	indexes := make([]string, 0, len(s.keys))
	for k := range s.keys {
		indexes = append(indexes, k)
	}
	sort.Strings(indexes)
	for _, k := range indexes {
		if err = ctx.Err(); err != nil {
			return err
		}
		v := s.keys[k]
		if err = s.writeUnflushed(record{Type: "key", Key: k, Cursor: v.cursor, Hash: v.hash}); err != nil {
			return err
		}
	}
	for p, id := range s.acks {
		if err = s.writeUnflushed(record{Type: "ack", Principal: p, Cursor: id}); err != nil {
			return err
		}
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = os.Rename(tmp, s.path); err != nil {
		return err
	}
	// Directory sync ensures the rename is durable after a machine crash.
	d, err := os.Open(dir)
	if err == nil {
		err = d.Sync()
		d.Close()
	}
	committed = true
	s.file = f
	_ = old.Close()
	// The named path is now the newly locked, open log file.
	return err
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	err := s.file.Sync()
	e := s.file.Close()
	if err != nil {
		return err
	}
	return e
}

func (s *Store) write(r record) error {
	if err := s.writeUnflushed(r); err != nil {
		return err
	}
	return s.file.Sync()
}
func (s *Store) writeUnflushed(r record) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(b) > maxRecord {
		return ErrLimit
	}
	var head [8]byte
	binary.BigEndian.PutUint32(head[:4], uint32(len(b)))
	binary.BigEndian.PutUint32(head[4:], crc32.Checksum(b, table))
	offset, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if _, err = s.file.Write(append(head[:], b...)); err != nil {
		_ = s.file.Truncate(offset)
		return err
	}
	return nil
}
func (s *Store) recover() error {
	sz, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	var off int64
	for off < sz {
		if sz-off < 8 {
			return s.file.Truncate(off)
		}
		var hdr [8]byte
		if _, err = s.file.ReadAt(hdr[:], off); err != nil {
			return err
		}
		n := int64(binary.BigEndian.Uint32(hdr[:4]))
		if n <= 0 || n > maxRecord {
			return fmt.Errorf("%w at offset %d", ErrCorrupt, off)
		}
		if sz-off-8 < n {
			return s.file.Truncate(off)
		}
		b := make([]byte, n)
		if _, err = s.file.ReadAt(b, off+8); err != nil {
			return err
		}
		if crc32.Checksum(b, table) != binary.BigEndian.Uint32(hdr[4:]) {
			return fmt.Errorf("%w at offset %d", ErrCorrupt, off)
		}
		var r record
		if err = json.Unmarshal(b, &r); err != nil {
			return fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
		if err = s.apply(r); err != nil {
			return fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
		off += 8 + n
	}
	return nil
}
func (s *Store) apply(r record) error {
	switch r.Type {
	case "append":
		if r.Cursor != s.next+1 || r.Principal.Subject == "" {
			return errors.New("invalid append sequence or principal")
		}
		s.next = r.Cursor
		s.items[r.Cursor] = item{r.Principal, r.Message, r.Key}
		s.byPrincipal[r.Principal] = append(s.byPrincipal[r.Principal], r.Cursor)
		s.liveBytes += int64(len(r.Message.Payload))
		if r.Key != "" {
			b, _ := json.Marshal(struct {
				P protocol.Principal
				M protocol.Message
			}{r.Principal, r.Message})
			s.keys[indexKey(r.Principal, r.Key)] = keyRecord{r.Cursor, sum256(b)}
		}
	case "checkpoint":
		if r.Cursor < s.next {
			return errors.New("checkpoint rewinds counter")
		}
		s.next = r.Cursor
	case "restore":
		if r.Cursor == 0 || r.Cursor > s.next {
			return errors.New("invalid restored cursor")
		}
		if _, exists := s.items[r.Cursor]; exists {
			return errors.New("duplicate restored cursor")
		}
		s.items[r.Cursor] = item{r.Principal, r.Message, r.Key}
		s.byPrincipal[r.Principal] = append(s.byPrincipal[r.Principal], r.Cursor)
		s.liveBytes += int64(len(r.Message.Payload))
	case "key":
		if r.Key == "" || r.Cursor > s.next {
			return errors.New("invalid key index")
		}
		s.keys[r.Key] = keyRecord{r.Cursor, r.Hash}
	case "ack":
		if r.Cursor > s.next {
			return errors.New("future ack")
		}
		if r.Cursor <= s.acks[r.Principal] {
			return nil
		}
		s.acks[r.Principal] = r.Cursor
		s.pruneIDs(r.Principal, r.Cursor)
		for n, it := range s.items {
			if n <= r.Cursor && it.principal == r.Principal {
				s.liveBytes -= int64(len(it.message.Payload))
				delete(s.items, n)
			}
		}
	default:
		return errors.New("unknown record type")
	}
	return nil
}
func (s *Store) pruneIDs(p protocol.Principal, cursor uint64) {
	ids := s.byPrincipal[p]
	count := sort.Search(len(ids), func(i int) bool { return ids[i] > cursor })
	if count == 0 {
		return
	}
	if count == len(ids) {
		delete(s.byPrincipal, p)
		return
	}
	s.byPrincipal[p] = append([]uint64(nil), ids[count:]...)
}
func clone(m protocol.Message) protocol.Message {
	m.Payload = append([]byte(nil), m.Payload...)
	return m
}
func maxUint(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
func indexKey(p protocol.Principal, k string) string {
	b, _ := json.Marshal([]string{p.Tenant, p.Subject, p.Device, k})
	return string(b)
}
