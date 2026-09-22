package platform

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"sync/atomic"
	"time"
)

// Identifiers generated here are used for run ids, task ids, job ids, idempotency
// scopes and audit records. They are all crypto/rand backed rather than
// sequential: an id that leaks ordering also leaks volume, and a guessable run
// id is a way to read somebody else's order.

var idCounter atomic.Uint64

// newRandomID returns a 128-bit random identifier in RFC 4122 v4 form. It reads
// from crypto/rand and panics only if the operating system's entropy source
// fails, which is not a condition a request handler can meaningfully recover
// from.
func newRandomID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic("ref/platform: crypto/rand is unavailable: " + err.Error())
	}
	buf[6] = (buf[6] & 0x0f) | 0x40
	buf[8] = (buf[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(buf[:])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:]
}

// newPrefixedID returns a sortable, prefixed identifier: a millisecond
// timestamp, a process-local counter and 8 random bytes.
//
// The leading timestamp makes ids cluster in a B-tree index in insertion order,
// which keeps a hot runs table from fragmenting; the random tail keeps them
// unguessable, and the counter keeps two ids minted in the same millisecond
// distinct without a lock.
func newPrefixedID(prefix string) string {
	var buf [20]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(time.Now().UTC().UnixMilli()))
	binary.BigEndian.PutUint32(buf[8:12], uint32(idCounter.Add(1)))
	if _, err := rand.Read(buf[12:]); err != nil {
		panic("ref/platform: crypto/rand is unavailable: " + err.Error())
	}
	encoded := hex.EncodeToString(buf[:])
	if prefix == "" {
		return encoded
	}
	return prefix + "_" + encoded
}

// newToken returns a URL-safe random token of the requested byte strength,
// used for password resets, API keys and resume tokens.
func newToken(bytes int) string {
	if bytes <= 0 {
		bytes = 32
	}
	buf := make([]byte, bytes)
	if _, err := rand.Read(buf); err != nil {
		panic("ref/platform: crypto/rand is unavailable: " + err.Error())
	}
	return strings.TrimRight(base64URL.EncodeToString(buf), "=")
}

// base64URL is the alphabet used for tokens that travel in URLs and headers;
// base64Std decodes the standard-alphabet values HTTP itself uses, such as a
// basic Authorization header.
var (
	base64URL = base64.URLEncoding
	base64Std = base64.StdEncoding
)
