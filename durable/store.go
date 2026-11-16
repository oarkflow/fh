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

const (
	maxRecord             = 2 << 20
	maxIdempotencyKeys    = 100_000
	maxAckPrincipals      = 100_000
	maxPrincipalFieldSize = 256
	maxIndexKeySize       = 8 << 10
	maxPrincipalIndexHead = 16
	liveRecordIndexCharge = 256 // Approximate retained map/index overhead per live record.
)

var table = crc32.MakeTable(crc32.Castagnoli)
var (
	ErrClosed   = errors.New("durable: closed")
	ErrConflict = errors.New("durable: idempotency key reused for different content")
	ErrCorrupt  = errors.New("durable: corrupt committed log entry")
	ErrLimit    = errors.New("durable: storage capacity reached")
	// ErrUncertain indicates that WAL I/O may have reached disk but did not
	// complete durably; the Store is poisoned and must be reopened.
	ErrUncertain = errors.New("durable: WAL I/O outcome uncertain")
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
// Its in-memory index uses a bounded accounting budget: payload bytes plus a
// fixed per-record index charge count toward MaxLiveBytes. Use per-tenant
// sharding for scale.
type Store struct {
	mu            sync.Mutex
	file          *os.File
	path          string
	closed        bool
	writeErr      error
	maxLiveBytes  int64
	liveBytes     int64
	next          uint64
	items         map[uint64]item
	byPrincipal   map[protocol.Principal][]uint64
	principalHead map[protocol.Principal]int
	acks          map[protocol.Principal]uint64
	keys          map[string]keyRecord
}

func Open(path string, maxLiveBytes int64) (*Store, error) {
	if maxLiveBytes <= 0 {
		maxLiveBytes = 64 << 20
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	before, err := os.Lstat(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err == nil && !before.Mode().IsRegular() {
		return nil, errors.New("durable: WAL path must be a regular file, not a symlink")
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	opened, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	current, err := os.Lstat(path)
	if err != nil {
		f.Close()
		return nil, err
	}
	if !opened.Mode().IsRegular() || !current.Mode().IsRegular() ||
		opened.Mode().Perm()&0077 != 0 || !os.SameFile(opened, current) ||
		(before != nil && !os.SameFile(before, opened)) {
		f.Close()
		return nil, errors.New("durable: WAL must be a stable regular file with mode 0600 or stricter")
	}
	if err := lockExclusive(f); err != nil {
		f.Close()
		return nil, fmt.Errorf("durable: exclusive file lock: %w", err)
	}
	st := &Store{file: f, path: path, maxLiveBytes: maxLiveBytes, items: make(map[uint64]item), byPrincipal: make(map[protocol.Principal][]uint64), principalHead: make(map[protocol.Principal]int), acks: make(map[protocol.Principal]uint64), keys: make(map[string]keyRecord)}
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
	if err := validatePrincipal(p); err != nil {
		return 0, err
	}
	if key != "" && len(key) > 256 {
		return 0, errors.New("durable: idempotency key too long")
	}
	if len(m.Payload) > 1<<20 {
		return 0, ErrLimit
	}
	cost := liveRecordCost(m)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := s.usableLocked(); err != nil {
		return 0, err
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
	clear(payload)
	index := indexKey(p, key)
	if key != "" {
		if v, ok := s.keys[index]; ok {
			if v.hash != h {
				return 0, ErrConflict
			}
			return v.cursor, nil
		}
	}
	if cost > s.maxLiveBytes-s.liveBytes || (key != "" && len(s.keys) >= maxIdempotencyKeys) {
		return 0, ErrLimit
	}
	cursor := s.next + 1
	if cursor == 0 {
		return 0, ErrLimit
	}
	r := record{Type: "append", Principal: p, Cursor: cursor, Message: clone(m), Key: key}
	if err := s.write(r); err != nil {
		return 0, err
	}
	s.next = cursor
	s.items[cursor] = item{p, clone(m), key}
	s.byPrincipal[p] = append(s.byPrincipal[p], cursor)
	s.liveBytes += cost
	if key != "" {
		s.keys[index] = keyRecord{cursor, h}
	}
	return cursor, nil
}
func (s *Store) ReadAfter(ctx context.Context, p protocol.Principal, cursor uint64, max int) ([]protocol.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validatePrincipal(p); err != nil {
		return nil, err
	}
	if max <= 0 || max > 1024 {
		return nil, errors.New("durable: max must be 1..1024")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.usableLocked(); err != nil {
		return nil, err
	}
	out := make([]protocol.Message, 0, min(max, 16))
	limit := maxUint(cursor, s.acks[p])
	ids := s.byPrincipal[p]
	head := s.principalHead[p]
	offset := head + sort.Search(len(ids)-head, func(i int) bool { return ids[head+i] > limit })
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
	if err := validatePrincipal(p); err != nil {
		return nil, err
	}
	if max <= 0 || max > 1024 {
		return nil, errors.New("durable: max must be 1..1024")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.usableLocked(); err != nil {
		return nil, err
	}
	out := make([]Entry, 0, min(max, 16))
	limit := maxUint(cursor, s.acks[p])
	ids := s.byPrincipal[p]
	head := s.principalHead[p]
	offset := head + sort.Search(len(ids)-head, func(i int) bool { return ids[head+i] > limit })
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
	if err := validatePrincipal(p); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.usableLocked(); err != nil {
		return err
	}
	if cursor > s.next {
		return errors.New("durable: cursor beyond committed log")
	}
	if cursor <= s.acks[p] {
		return nil
	}
	if _, exists := s.acks[p]; !exists && len(s.acks) >= maxAckPrincipals {
		return ErrLimit
	}
	if err := s.write(record{Type: "ack", Principal: p, Cursor: cursor}); err != nil {
		return err
	}
	s.acks[p] = cursor
	s.pruneAcknowledged(p, cursor)
	return nil
}

func (s *Store) pruneAcknowledged(p protocol.Principal, cursor uint64) {
	ids := s.byPrincipal[p]
	head := s.principalHead[p]
	count := sort.Search(len(ids)-head, func(i int) bool { return ids[head+i] > cursor })
	for _, n := range ids[head : head+count] {
		if it, ok := s.items[n]; ok {
			s.liveBytes -= liveRecordCost(it.message)
			clear(it.message.Payload)
			delete(s.items, n)
		}
	}
	s.pruneIDs(p, cursor)
}

// ReplayCursor is the last fsynced application acknowledgement for this identity.
func (s *Store) ReplayCursor(p protocol.Principal) (uint64, error) {
	if err := validatePrincipal(p); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usableLocked(); err != nil {
		return 0, err
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
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.usableLocked(); err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	f, err := os.CreateTemp(dir, ".mira-compact-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if err = f.Chmod(0600); err != nil {
		f.Close()
		os.Remove(f.Name())
		return err
	}
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
	if err != nil {
		s.writeErr = fmt.Errorf("%w: compacted WAL rename: %w", ErrUncertain, err)
		return s.writeErr
	}
	return nil
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
	for _, it := range s.items {
		clear(it.message.Payload)
	}
	clear(s.items)
	clear(s.byPrincipal)
	clear(s.principalHead)
	clear(s.acks)
	clear(s.keys)
	s.liveBytes = 0
	if err != nil {
		return err
	}
	if e != nil {
		return e
	}
	return s.writeErr
}

func (s *Store) write(r record) error {
	if err := s.writeUnflushed(r); err != nil {
		if errors.Is(err, ErrUncertain) {
			s.writeErr = err
		}
		return err
	}
	if err := s.file.Sync(); err != nil {
		s.writeErr = fmt.Errorf("%w: fsync: %w", ErrUncertain, err)
		return s.writeErr
	}
	return nil
}
func (s *Store) writeUnflushed(r record) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	defer clear(b)
	if len(b) > maxRecord {
		return ErrLimit
	}
	var head [8]byte
	binary.BigEndian.PutUint32(head[:4], uint32(len(b)))
	binary.BigEndian.PutUint32(head[4:], crc32.Checksum(b, table))
	offset, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("%w: seek: %w", ErrUncertain, err)
	}
	frame := make([]byte, len(head)+len(b))
	defer clear(frame)
	copy(frame, head[:])
	copy(frame[len(head):], b)
	n, err := s.file.Write(frame)
	if err != nil || n != len(frame) {
		truncateErr := s.file.Truncate(offset)
		if err == nil {
			err = io.ErrShortWrite
		}
		if truncateErr != nil {
			return fmt.Errorf("%w: write: %w (truncate failed: %v)", ErrUncertain, err, truncateErr)
		}
		return fmt.Errorf("%w: write: %w", ErrUncertain, err)
	}
	return nil
}

// usableLocked rejects every operation after ambiguous WAL I/O. Continuing
// after a failed write or fsync could let the in-memory cursor diverge from
// records that may later be recovered from disk.
func (s *Store) usableLocked() error {
	if s.closed {
		return ErrClosed
	}
	if s.writeErr != nil {
		return s.writeErr
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
			clear(b)
			return err
		}
		if crc32.Checksum(b, table) != binary.BigEndian.Uint32(hdr[4:]) {
			clear(b)
			return fmt.Errorf("%w at offset %d", ErrCorrupt, off)
		}
		var r record
		if err = json.Unmarshal(b, &r); err != nil {
			clear(b)
			return fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
		clear(b)
		if err = s.apply(r); err != nil {
			clear(r.Message.Payload)
			return fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
		off += 8 + n
	}
	return nil
}
func (s *Store) apply(r record) error {
	switch r.Type {
	case "append":
		if r.Cursor == 0 || r.Cursor != s.next+1 || validatePrincipal(r.Principal) != nil || len(r.Message.Payload) > 1<<20 {
			return errors.New("invalid append sequence, principal, or payload size")
		}
		cost := liveRecordCost(r.Message)
		if cost > s.maxLiveBytes-s.liveBytes {
			return ErrLimit
		}
		if r.Key != "" {
			if len(r.Key) > 256 {
				return errors.New("idempotency key too long")
			}
			index := indexKey(r.Principal, r.Key)
			if _, exists := s.keys[index]; exists || len(s.keys) >= maxIdempotencyKeys {
				return errors.New("duplicate or excessive idempotency key")
			}
		}
		s.next = r.Cursor
		s.items[r.Cursor] = item{r.Principal, r.Message, r.Key}
		s.byPrincipal[r.Principal] = append(s.byPrincipal[r.Principal], r.Cursor)
		s.liveBytes += cost
		if r.Key != "" {
			b, err := json.Marshal(struct {
				P protocol.Principal
				M protocol.Message
			}{r.Principal, r.Message})
			if err != nil {
				return err
			}
			h := sum256(b)
			clear(b)
			s.keys[indexKey(r.Principal, r.Key)] = keyRecord{r.Cursor, h}
		}
	case "checkpoint":
		if r.Cursor < s.next {
			return errors.New("checkpoint rewinds counter")
		}
		s.next = r.Cursor
	case "restore":
		if r.Cursor == 0 || r.Cursor > s.next || validatePrincipal(r.Principal) != nil || len(r.Message.Payload) > 1<<20 || len(r.Key) > 256 {
			return errors.New("invalid restored cursor")
		}
		if _, exists := s.items[r.Cursor]; exists {
			return errors.New("duplicate restored cursor")
		}
		cost := liveRecordCost(r.Message)
		if cost > s.maxLiveBytes-s.liveBytes {
			return ErrLimit
		}
		s.items[r.Cursor] = item{r.Principal, r.Message, r.Key}
		s.byPrincipal[r.Principal] = append(s.byPrincipal[r.Principal], r.Cursor)
		s.liveBytes += cost
	case "key":
		if r.Key == "" || len(r.Key) > maxIndexKeySize || r.Cursor == 0 || r.Cursor > s.next {
			return errors.New("invalid key index")
		}
		if _, exists := s.keys[r.Key]; exists || len(s.keys) >= maxIdempotencyKeys {
			return errors.New("duplicate or excessive key index")
		}
		s.keys[r.Key] = keyRecord{r.Cursor, r.Hash}
	case "ack":
		if r.Cursor > s.next || validatePrincipal(r.Principal) != nil {
			return errors.New("invalid ack cursor or principal")
		}
		if r.Cursor <= s.acks[r.Principal] {
			return nil
		}
		if _, exists := s.acks[r.Principal]; !exists && len(s.acks) >= maxAckPrincipals {
			return errors.New("too many ack principals")
		}
		s.acks[r.Principal] = r.Cursor
		s.pruneAcknowledged(r.Principal, r.Cursor)
	default:
		return errors.New("unknown record type")
	}
	return nil
}
func (s *Store) pruneIDs(p protocol.Principal, cursor uint64) {
	ids := s.byPrincipal[p]
	head := s.principalHead[p]
	count := sort.Search(len(ids)-head, func(i int) bool { return ids[head+i] > cursor })
	if count == 0 {
		return
	}
	head += count
	if head == len(ids) {
		delete(s.byPrincipal, p)
		delete(s.principalHead, p)
		return
	}
	if head >= maxPrincipalIndexHead && head*2 >= len(ids) {
		s.byPrincipal[p] = append([]uint64(nil), ids[head:]...)
		delete(s.principalHead, p)
		return
	}
	s.principalHead[p] = head
}
func clone(m protocol.Message) protocol.Message {
	m.Payload = append([]byte(nil), m.Payload...)
	return m
}
func validatePrincipal(p protocol.Principal) error {
	if p.Subject == "" || len(p.Subject) > maxPrincipalFieldSize || len(p.Tenant) > maxPrincipalFieldSize || len(p.Device) > maxPrincipalFieldSize {
		return errors.New("durable: principal fields must be at most 256 bytes and subject is required")
	}
	return nil
}
func liveRecordCost(m protocol.Message) int64 {
	return int64(len(m.Payload) + liveRecordIndexCharge)
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
