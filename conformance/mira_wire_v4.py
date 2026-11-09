"""Independent Python 3 stdlib parser for MIRA v4's *unencrypted framing*.
This is a second codec implementation, NOT a complete independent transport.
"""
import struct

MAGIC = 0x4D54
VERSION = 4
HEADER = struct.Struct('!HBBBBQQHH')
MAX_DATAGRAM = 1472


def header(kind: int, connection: int, number: int, epoch: int, length: int, flags: int = 0) -> bytes:
    if not (1 <= kind <= 6 and 0 <= flags <= 255 and 0 <= length <= 65535):
        raise ValueError('invalid header fields')
    return HEADER.pack(MAGIC, VERSION, kind, flags, 26, connection, number, epoch, length)


def parse_header(raw: bytes) -> dict:
    if len(raw) < HEADER.size or len(raw) > MAX_DATAGRAM:
        raise ValueError('invalid packet size')
    magic, version, kind, flags, length_of_header, connection, number, epoch, body_len = HEADER.unpack(raw[:HEADER.size])
    if magic != MAGIC or version != VERSION or length_of_header != 26 or not 1 <= kind <= 6:
        raise ValueError('bad header')
    if HEADER.size + body_len != len(raw):
        raise ValueError('bad body length')
    if kind == 5 and body_len < 16:
        raise ValueError('truncated AEAD')
    return dict(kind=kind, flags=flags, connection=connection, number=number, epoch=epoch, body=raw[26:])


def uvarint(n: int) -> bytes:
    if not 0 <= n <= (1 << 64) - 1:
        raise ValueError('invalid varint')
    result = bytearray()
    while n >= 128:
        result.append((n & 127) | 128)
        n >>= 7
    result.append(n)
    return bytes(result)


def read_varint(raw: bytes, pos: int) -> tuple[int, int]:
    value = 0
    start = pos
    for i in range(10):
        if pos >= len(raw):
            raise ValueError('truncated varint')
        b = raw[pos]
        pos += 1
        if i == 9 and b > 1:
            raise ValueError('overflow')
        value |= (b & 127) << (7 * i)
        if not b & 128:
            if raw[start:pos] != uvarint(value):
                raise ValueError('noncanonical varint')
            return value, pos
    raise ValueError('varint too long')


def frame(typ: int, body: bytes, flags: int = 0) -> bytes:
    if not 1 <= typ <= 17 or len(body) > 1048576:
        raise ValueError('invalid frame')
    return bytes((typ, flags)) + uvarint(len(body)) + body


def frames(raw: bytes) -> list[tuple[int, int, bytes]]:
    out = []
    pos = 0
    while pos < len(raw):
        if len(out) >= 1024 or len(raw) - pos < 3:
            raise ValueError('invalid frame count/length')
        typ, flags = raw[pos], raw[pos + 1]
        if not 1 <= typ <= 17:
            raise ValueError('frame type')
        size, pos = read_varint(raw, pos + 2)
        if size > 1048576 or pos + size > len(raw):
            raise ValueError('frame size')
        out.append((typ, flags, raw[pos:pos + size]))
        pos += size
    return out
