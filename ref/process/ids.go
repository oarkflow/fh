package process

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"sync/atomic"
	"time"
)

// Identifiers.
//
// Run, task, timer and subscription ids are crypto/rand backed and prefixed with a
// millisecond timestamp. The timestamp makes them cluster in insertion order in a
// B-tree index, which keeps a hot runs table from fragmenting; the random tail
// keeps them unguessable, which matters because a run id appears in URLs and a
// guessable one is a way to read somebody else's order.

var idCounter atomic.Uint64

// randomID returns a sortable, unguessable identifier.
// Returns empty string on crypto/rand failure instead of panicking.
func randomID() string {
	var buf [20]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(time.Now().UTC().UnixMilli()))
	binary.BigEndian.PutUint32(buf[8:12], uint32(idCounter.Add(1)))
	if _, err := rand.Read(buf[12:]); err != nil {
		// Fallback: use timestamp + counter + zeros rather than crashing.
		// The ID is less random but still unique within this process.
		return hex.EncodeToString(buf[:])
	}
	return hex.EncodeToString(buf[:])
}

// orRandomID returns the caller's id, or a prefixed random one.
func orRandomID(id, prefix string) string {
	if id != "" {
		return id
	}
	if prefix == "" {
		return randomID()
	}
	return prefix + "_" + randomID()
}
