// Package compress provides modern compression encoders (Brotli, Zstd) as
// drop-in replacements for the gzip encoder in mw/compress. These encoders
// implement the compress.Encoder interface and can be used directly.
//
// Usage with mw/compress:
//
//	import (
//	    "github.com/oarkflow/fh/mw/compress"
//	    "github.com/oarkflow/fh/contrib/compress"
//	)
//
//	app.Use(compress.New(compress.Config{
//	    Encoder: compress2.NewBrotliEncoder(brotli.DefaultCompression),
//	}))
//
// Brotli provides 15-25% better compression than gzip at similar speeds.
// Zstd provides similar compression to Brotli but with 3-5x faster decompression.
package compress

import (
	"bytes"
	"io"
	"sync"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
)

const (
	EncodingBrotli = "br"
	EncodingZstd   = "zstd"
)

// BrotliEncoder wraps the brotli writer with pool recycling.
// Level should be 1-11 (1=fastest, 11=best compression).
type BrotliEncoder struct {
	level int
	pool  sync.Pool
}

// NewBrotliEncoder creates a new brotli encoder.
func NewBrotliEncoder(level int) *BrotliEncoder {
	if level == 0 {
		level = brotli.DefaultCompression
	}
	e := &BrotliEncoder{level: level}
	e.pool.New = func() any {
		return brotli.NewWriterLevel(io.Discard, level)
	}
	return e
}

func (e *BrotliEncoder) Encoding() string { return EncodingBrotli }

func (e *BrotliEncoder) Encode(dst *bytes.Buffer, src []byte) error {
	w := e.pool.Get().(*brotli.Writer)
	w.Reset(dst)
	_, writeErr := w.Write(src)
	closeErr := w.Close()
	w.Reset(io.Discard)
	e.pool.Put(w)
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

// ZstdEncoder wraps the zstd encoder with pool recycling.
type ZstdEncoder struct {
	level zstd.EncoderLevel
	pool  sync.Pool
	once  sync.Once
}

// NewZstdEncoder creates a new zstd encoder.
// Use zstd.SpeedFastest, zstd.SpeedDefault, or zstd.SpeedBestCompression.
func NewZstdEncoder(level zstd.EncoderLevel) *ZstdEncoder {
	if level == 0 {
		level = zstd.SpeedFastest
	}
	return &ZstdEncoder{level: level}
}

func (e *ZstdEncoder) Encoding() string { return EncodingZstd }

func (e *ZstdEncoder) Encode(dst *bytes.Buffer, src []byte) error {
	e.once.Do(func() {
		// Each pooled encoder is independent so concurrent Encode calls never
		// share one *zstd.Encoder (which is not safe for concurrent use).
		e.pool.New = func() any {
			enc, err := zstd.NewWriter(io.Discard,
				zstd.WithEncoderLevel(e.level),
				zstd.WithEncoderConcurrency(1),
			)
			if err != nil {
				panic("compress: zstd encoder init failed: " + err.Error())
			}
			return enc
		}
	})

	enc := e.pool.Get().(*zstd.Encoder)
	enc.Reset(dst)
	_, writeErr := enc.Write(src)
	closeErr := enc.Close()
	enc.Reset(io.Discard)
	e.pool.Put(enc)
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}
