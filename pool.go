package fasthttp

import (
	"sync"
)

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

func putBytes(b *[]byte) {
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
