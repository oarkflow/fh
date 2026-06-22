package hpack

import (
	"sync"
)

// ── Huffman encoding ─────────────────────────────────────────────────────────

// AppendHuffmanString appends s Huffman-encoded to dst.
//
// Codegen notes (Go 1.22 amd64):
//
//  1. `n += uint(huffmanCodeLen[c])` before `x <<= huffmanCodeLen[c] % 64`
//     lets the compiler reuse the already-loaded codeLen byte for both the
//     addition and the shift without an extra register move.
//
//  2. `x <<= huffmanCodeLen[c] % 64` — the `% 64` is a hint that the shift
//     amount is always in [0,63], eliminating the branchless overflow guard
//     (CMPQ/SBBQ/ANDQ sequence) the compiler emits for an unbounded shift.
//
//  3. `n %= 32` instead of `n -= 32` after the flush — same hint: tells the
//     compiler n is in [0,31] for the subsequent `x >> n` shift, removing
//     another guard sequence.
//
//  4. Case 2/3 of the tail flush use uint16 intermediates so the compiler
//     emits ROLW + a single wide store rather than two independent byte stores.
//
// Max Huffman code length is 30 bits (see huffmanCodeLen), so the 64-bit
// accumulator always has room for at least one more code before a flush.
func AppendHuffmanString(dst []byte, s string) []byte {
	var (
		x uint64 // bit accumulator
		n uint   // valid bits in x
	)
	for i := 0; i < len(s); i++ {
		c := s[i]
		n += uint(huffmanCodeLen[c])
		x <<= huffmanCodeLen[c] % 64 // % 64 removes compiler shift-overflow guard
		x |= uint64(huffmanCodes[c])
		if n >= 32 {
			n %= 32             // %= 32 informs compiler 0 <= n <= 31 for shift below
			y := uint32(x >> n) // uint32: compiler emits BSWAP + single 4-byte MOV
			dst = append(dst, byte(y>>24), byte(y>>16), byte(y>>8), byte(y))
		}
	}
	// Pad remaining bits with EOS prefix (all-1s, RFC 7541 §5.2).
	if over := n % 8; over > 0 {
		const (
			eosCode    = 0x3fffffff
			eosNBits   = 30
			eosPadByte = eosCode >> (eosNBits - 8) // = 0xff
		)
		pad := 8 - over
		x = (x << pad) | (eosPadByte >> over)
		n += pad // n is now a multiple of 8 in {8,16,24,32}
	}
	// Flush 0-4 remaining bytes. uint16 intermediates let the compiler use
	// ROLW and emit 2-byte stores instead of two independent MOVB instructions.
	switch n / 8 {
	case 0:
		return dst
	case 1:
		return append(dst, byte(x))
	case 2:
		y := uint16(x)
		return append(dst, byte(y>>8), byte(y))
	case 3:
		y := uint16(x >> 8)
		return append(dst, byte(y>>8), byte(y), byte(x))
	}
	// case 4:
	y := uint32(x)
	return append(dst, byte(y>>24), byte(y>>16), byte(y>>8), byte(y))
}

// HuffmanEncodeLength returns the number of bytes required to Huffman-encode s,
// rounded up to the next byte boundary.
func HuffmanEncodeLength(s string) uint64 {
	n := uint64(0)
	for i := 0; i < len(s); i++ {
		n += uint64(huffmanCodeLen[s[i]])
	}
	return (n + 7) / 8
}

// ── Huffman decoding ─────────────────────────────────────────────────────────

// huffmanDecode decodes Huffman-encoded src, appending decoded bytes to dst.
// If maxLen > 0, returns ErrStringLength if the decoded length would exceed it.
//
// Design: flat 256-entry dispatch table at each tree level. The hot path (main
// loop) processes 8 bits per iteration with a single array lookup and no pointer
// chasing. The tail handles 1-7 leftover bits after all input bytes are consumed.
//
// Padding semantics (RFC 7541 §5.2): after the last symbol, remaining bits must
// be a prefix of the EOS code (all-1s). We detect this by checking whether the
// leaf's codeLen exceeds the number of bits accumulated since the last decoded
// symbol (sbits). If so, the bits are a padding prefix, not a real symbol.
func huffmanDecode(dst []byte, maxLen int, src []byte) ([]byte, error) {
	root := getRootHuffmanNode()
	n := root

	// cur accumulates raw input bits (low cbits bits are valid).
	// sbits counts bits consumed in the current (possibly partial) symbol path,
	// used to distinguish EOS padding from a real symbol in the tail.
	var cur uint
	var cbits, sbits uint8
	var totalCodeBits int

	for _, b := range src {
		cur = cur<<8 | uint(b)
		cbits += 8
		sbits += 8

		for cbits >= 8 {
			idx := byte(cur >> (cbits - 8))
			child := n.children[idx]
			if child == nil {
				return dst, ErrInvalidHuffman
			}
			if child.children == nil {
				if maxLen != 0 && len(dst) == maxLen {
					return dst, ErrStringLength
				}
				dst = append(dst, child.sym)
				cbits -= child.codeLen
				totalCodeBits += int(child.codeLen)
				n = root
				sbits = cbits
			} else {
				cbits -= 8
				totalCodeBits += 8
				n = child
			}
		}
	}

	for cbits > 0 {
		idx := byte(cur << (8 - cbits))
		child := n.children[idx]
		if child == nil {
			return dst, ErrInvalidHuffman
		}
		if child.children == nil {
			if sbits < child.codeLen {
				break
			}
			if maxLen != 0 && len(dst) == maxLen {
				return dst, ErrStringLength
			}
			dst = append(dst, child.sym)
			cbits -= child.codeLen
			totalCodeBits += int(child.codeLen)
			n = root
			sbits = cbits
		} else {
			break
		}
	}

	// RFC 7541 §5.2: padding longer than 7 bits is invalid.
	totalInputBits := len(src) * 8
	if padding := totalInputBits - totalCodeBits; padding > 7 || padding < 0 {
		return dst, ErrInvalidHuffman
	}

	if n != root {
		return dst, ErrInvalidHuffman
	}

	if sbits < cbits {
		return dst, ErrInvalidHuffman
	}
	if cbits > 0 {
		padBits := byte(cur << (8 - cbits))
		allOnes := byte(0xFF << (8 - cbits))
		if padBits&allOnes != allOnes {
			return dst, ErrInvalidHuffman
		}
	}

	return dst, nil
}

// HuffmanDecode decodes v into dst, returning the number of bytes written.
func HuffmanDecode(dst []byte, v []byte) ([]byte, error) {
	return huffmanDecode(dst, 0, v)
}

// HuffmanDecodeToString decodes v and returns the result as a string.
func HuffmanDecodeToString(v []byte) (string, error) {
	buf := decodeBufPool.Get().(*[]byte)
	*buf = (*buf)[:0]
	out, err := huffmanDecode(*buf, 0, v)
	s := string(out)
	*buf = out[:0]
	decodeBufPool.Put(buf)
	return s, err
}

// ── Huffman tree (built once) ────────────────────────────────────────────────

// node is a Huffman trie node.
// Internal nodes have a non-nil children pointer.
// Leaf nodes store the decoded symbol and its code length.
type node struct {
	children *[256]*node
	sym      byte
	codeLen  uint8
}

var (
	buildOnce sync.Once
	rootNode  *node
)

func getRootHuffmanNode() *node {
	buildOnce.Do(buildHuffmanTree)
	return rootNode
}

func buildHuffmanTree() {
	rootNode = &node{children: new([256]*node)}
	leaves := make([]node, 256)

	for sym := 0; sym < 256; sym++ {
		code := huffmanCodes[sym]
		codeLen := huffmanCodeLen[sym]

		cur := rootNode
		remaining := codeLen
		for remaining > 8 {
			remaining -= 8
			i := uint8(code >> remaining)
			if cur.children[i] == nil {
				cur.children[i] = &node{children: new([256]*node)}
			}
			cur = cur.children[i]
		}

		shift := 8 - remaining
		start := int(uint8(code << shift))
		count := 1 << shift

		leaves[sym].sym = byte(sym)
		leaves[sym].codeLen = remaining
		for i := start; i < start+count; i++ {
			cur.children[i] = &leaves[sym]
		}
	}
}

// ── Huffman code tables (RFC 7541 Appendix B) ────────────────────────────────

var huffmanCodes = [256]uint32{
	0x1ff8, 0x7fffd8, 0xfffffe2, 0xfffffe3, 0xfffffe4, 0xfffffe5, 0xfffffe6, 0xfffffe7,
	0xfffffe8, 0xffffea, 0x3ffffffc, 0xfffffe9, 0xfffffea, 0x3ffffffd, 0xfffffeb, 0xfffffec,
	0xfffffed, 0xfffffee, 0xfffffef, 0xffffff0, 0xffffff1, 0xffffff2, 0x3ffffffe, 0xffffff3,
	0xffffff4, 0xffffff5, 0xffffff6, 0xffffff7, 0xffffff8, 0xffffff9, 0xffffffa, 0xffffffb,
	0x14, 0x3f8, 0x3f9, 0xffa, 0x1ff9, 0x15, 0xf8, 0x7fa,
	0x3fa, 0x3fb, 0xf9, 0x7fb, 0xfa, 0x16, 0x17, 0x18,
	0x0, 0x1, 0x2, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
	0x1e, 0x1f, 0x5c, 0xfb, 0x7ffc, 0x20, 0xffb, 0x3fc,
	0x1ffa, 0x21, 0x5d, 0x5e, 0x5f, 0x60, 0x61, 0x62,
	0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a,
	0x6b, 0x6c, 0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72,
	0xfc, 0x73, 0xfd, 0x1ffb, 0x7fff0, 0x1ffc, 0x3ffc, 0x22,
	0x7ffd, 0x3, 0x23, 0x4, 0x24, 0x5, 0x25, 0x26,
	0x27, 0x6, 0x74, 0x75, 0x28, 0x29, 0x2a, 0x7,
	0x2b, 0x76, 0x2c, 0x8, 0x9, 0x2d, 0x77, 0x78,
	0x79, 0x7a, 0x7b, 0x7ffe, 0x7fc, 0x3ffd, 0x1ffd, 0xffffffc,
	0xfffe6, 0x3fffd2, 0xfffe7, 0xfffe8, 0x3fffd3, 0x3fffd4, 0x3fffd5, 0x7fffd9,
	0x3fffd6, 0x7fffda, 0x7fffdb, 0x7fffdc, 0x7fffdd, 0x7fffde, 0xffffeb, 0x7fffdf,
	0xffffec, 0xffffed, 0x3fffd7, 0x7fffe0, 0xffffee, 0x7fffe1, 0x7fffe2, 0x7fffe3,
	0x7fffe4, 0x1fffdc, 0x3fffd8, 0x7fffe5, 0x3fffd9, 0x7fffe6, 0x7fffe7, 0xffffef,
	0x3fffda, 0x1fffdd, 0xfffe9, 0x3fffdb, 0x3fffdc, 0x7fffe8, 0x7fffe9, 0x1fffde,
	0x7fffea, 0x3fffdd, 0x3fffde, 0xfffff0, 0x1fffdf, 0x3fffdf, 0x7fffeb, 0x7fffec,
	0x1fffe0, 0x1fffe1, 0x3fffe0, 0x1fffe2, 0x7fffed, 0x3fffe1, 0x7fffee, 0x7fffef,
	0xfffea, 0x3fffe2, 0x3fffe3, 0x3fffe4, 0x7ffff0, 0x3fffe5, 0x3fffe6, 0x7ffff1,
	0x3ffffe0, 0x3ffffe1, 0xfffeb, 0x7fff1, 0x3fffe7, 0x7ffff2, 0x3fffe8, 0x1ffffec,
	0x3ffffe2, 0x3ffffe3, 0x3ffffe4, 0x7ffffde, 0x7ffffdf, 0x3ffffe5, 0xfffff1, 0x1ffffed,
	0x7fff2, 0x1fffe3, 0x3ffffe6, 0x7ffffe0, 0x7ffffe1, 0x3ffffe7, 0x7ffffe2, 0xfffff2,
	0x1fffe4, 0x1fffe5, 0x3ffffe8, 0x3ffffe9, 0xffffffd, 0x7ffffe3, 0x7ffffe4, 0x7ffffe5,
	0xfffec, 0xfffff3, 0xfffed, 0x1fffe6, 0x3fffe9, 0x1fffe7, 0x1fffe8, 0x7ffff3,
	0x3fffea, 0x3fffeb, 0x1ffffee, 0x1ffffef, 0xfffff4, 0xfffff5, 0x3ffffea, 0x7ffff4,
	0x3ffffeb, 0x7ffffe6, 0x3ffffec, 0x3ffffed, 0x7ffffe7, 0x7ffffe8, 0x7ffffe9, 0x7ffffea,
	0x7ffffeb, 0xffffffe, 0x7ffffec, 0x7ffffed, 0x7ffffee, 0x7ffffef, 0x7fffff0, 0x3ffffee,
}

var huffmanCodeLen = [256]uint8{
	13, 23, 28, 28, 28, 28, 28, 28, 28, 24, 30, 28, 28, 30, 28, 28,
	28, 28, 28, 28, 28, 28, 30, 28, 28, 28, 28, 28, 28, 28, 28, 28,
	6, 10, 10, 12, 13, 6, 8, 11, 10, 10, 8, 11, 8, 6, 6, 6,
	5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 7, 8, 15, 6, 12, 10,
	13, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 8, 7, 8, 13, 19, 13, 14, 6,
	15, 5, 6, 5, 6, 5, 6, 6, 6, 5, 7, 7, 6, 6, 6, 5,
	6, 7, 6, 5, 5, 6, 7, 7, 7, 7, 7, 15, 11, 14, 13, 28,
	20, 22, 20, 20, 22, 22, 22, 23, 22, 23, 23, 23, 23, 23, 24, 23,
	24, 24, 22, 23, 24, 23, 23, 23, 23, 21, 22, 23, 22, 23, 23, 24,
	22, 21, 20, 22, 22, 23, 23, 21, 23, 22, 22, 24, 21, 22, 23, 23,
	21, 21, 22, 21, 23, 22, 23, 23, 20, 22, 22, 22, 23, 22, 22, 23,
	26, 26, 20, 19, 22, 23, 22, 25, 26, 26, 26, 27, 27, 26, 24, 25,
	19, 21, 26, 27, 27, 26, 27, 24, 21, 21, 26, 26, 28, 27, 27, 27,
	20, 24, 20, 21, 22, 21, 21, 23, 22, 22, 25, 25, 24, 24, 26, 23,
	26, 27, 26, 26, 27, 27, 27, 27, 27, 28, 27, 27, 27, 27, 27, 26,
}
