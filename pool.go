package fh

import (
	"strconv"
	"sync"
)

var contentLengthSmall [1024]string

const directHeaderCacheSize = 256

var (
	directPlain200Headers [directHeaderCacheSize][]byte
	directJSON200Headers  [directHeaderCacheSize][]byte
)

func init() {
	for i := range contentLengthSmall {
		contentLengthSmall[i] = "Content-Length: " + strconv.Itoa(i) + "\r\n"
	}
	for i := 0; i < directHeaderCacheSize; i++ {
		plain := make([]byte, 0, 96)
		plain = append(plain, "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\n"...)
		plain = append(plain, contentLengthSmall[i]...)
		plain = append(plain, '\r', '\n')
		directPlain200Headers[i] = plain

		json := make([]byte, 0, 80)
		json = append(json, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"...)
		json = append(json, contentLengthSmall[i]...)
		json = append(json, '\r', '\n')
		directJSON200Headers[i] = json
	}
}

var (
	bufPool512 = sync.Pool{New: func() any { b := make([]byte, 512); return &b }}
	bufPool4K  = sync.Pool{New: func() any { b := make([]byte, 4096); return &b }}
	bufPool16K = sync.Pool{New: func() any { b := make([]byte, 16384); return &b }}
	bufPool64K = sync.Pool{New: func() any { b := make([]byte, 65536); return &b }}
)

func getBuf(size int) *[]byte {
	switch {
	case size <= 512:
		return bufPool512.Get().(*[]byte)
	case size <= 4096:
		return bufPool4K.Get().(*[]byte)
	case size <= 16384:
		return bufPool16K.Get().(*[]byte)
	default:
		if size <= 65536 {
			return bufPool64K.Get().(*[]byte)
		}
		b := make([]byte, size)
		return &b
	}
}

func putBuf(b *[]byte) {
	switch cap(*b) {
	case 512:
		bufPool512.Put(b)
	case 4096:
		bufPool4K.Put(b)
	case 16384:
		bufPool16K.Put(b)
	case 65536:
		bufPool64K.Put(b)
	}
}

// bytesPool for response body building
var bytesPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 4096)
		return &b
	},
}

func getBytes() *[]byte {
	return bytesPool.Get().(*[]byte)
}

// maxPooledBytesCap bounds what putBytes will return to the shared pool.
// Without this, a single large response (echoed upload, large JSON dump,
// streamed body) permanently inflates the backing array of whichever pool
// slot it lands in, and that oversized buffer is then reused or sits idle
// across unrelated future requests — a sync.Pool memory-bloat pattern.
// Mirrors putBuf's fixed-size-only pooling below.
const maxPooledBytesCap = 1 << 20 // 1MB

func putBytes(b *[]byte) {
	if cap(*b) > maxPooledBytesCap {
		return
	}
	*b = (*b)[:0]
	bytesPool.Put(b)
}

// appendInt appends n (decimal) to dst without allocation.
// Uses a stack-allocated scratch buffer to avoid pool overhead.
func appendInt(dst []byte, n int) []byte {
	if n == 0 {
		return append(dst, '0')
	}
	if n < 0 {
		dst = append(dst, '-')
		n = -n
	}
	// Stack-allocate a scratch buffer (max 20 digits for 64-bit int)
	var buf [20]byte
	pos := 20
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return append(dst, buf[pos:]...)
}

func appendContentLengthLine(dst []byte, n int) []byte {
	if n >= 0 && n < len(contentLengthSmall) {
		return append(dst, contentLengthSmall[n]...)
	}
	dst = append(dst, "Content-Length: "...)
	dst = appendInt(dst, n)
	return append(dst, '\r', '\n')
}
