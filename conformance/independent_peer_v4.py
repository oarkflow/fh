#!/usr/bin/env python3
"""Independent, small Python MIRA v4 interoperability client (DEV PSK ONLY).

This runs only against the loopback legacy-PSK listener, not mTLS production.
Requires `cryptography`; packet/header/epoch/frames are independently encoded.
No claim of production cryptographic review. Run as a conformance smoke test.
"""
import argparse
import hashlib
import hmac
import os
import socket
import struct
import time
from ipaddress import ip_address
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives import serialization
from mira_wire_v4 import header, parse_header, uvarint, read_varint, frame, frames


def mac(key, content):
    return hmac.new(key, content, hashlib.sha256).digest()


def derive(master, cid, outbound):
    direction = b"c2s" if outbound else b"s2c"
    chain = mac(master, b"MIRA-T/1 v4 directional chain " + direction + cid)
    info = b"MIRA-T/1 traffic keys " + b"v4 " + direction + b":" + cid + b"\0\0"
    prk = mac(b"MIRA-T/1 hkdf extract salt", chain)
    out, block = b"", b""
    for counter in range(1, 3):
        block = mac(prk, block + info + bytes([counter]))
        out += block
    return out[:32], out[32:44]


def nonce(iv, pn):
    n = bytearray(iv)
    v = pn.to_bytes(8, "big")
    for i in range(8):
        n[4+i] ^= v[i]
    return bytes(n)


class Client:
    def __init__(self, host: str, port: int, secret: bytes):
        if not ip_address(socket.gethostbyname(host)).is_loopback:
            raise ValueError("independent PSK peer must be loopback-only")
        if len(secret) != 32:
            raise ValueError("32-byte random secret required")
        self.addr = (host, port)
        self.psk = secret
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(0.25)
        self.cid = int.from_bytes(os.urandom(8), "big") or 1
        self.sent_pn = 2
        self.tx = self.rx = None
        self.iv_tx = self.iv_rx = None

    def send_plain(self, kind, pn, payload):
        self.sock.sendto(header(kind, self.cid, pn, 0, len(payload)) + payload, self.addr)

    def send(self, typ, content=b"", reliable=True):
        pn = self.sent_pn
        self.sent_pn += 1
        if pn >= 4096:
            raise RuntimeError("Python smoke peer only supports key epoch 0")
        payload = frame(typ, content)
        h = header(5, self.cid, pn, 0, len(payload) + 16, int(reliable))
        self.sock.sendto(h + AESGCM(self.tx).encrypt(nonce(self.iv_tx, pn), payload, h), self.addr)
        return pn

    def packet(self):
        raw, sender = self.sock.recvfrom(2048)
        if sender != self.addr:
            return None
        parsed = parse_header(raw)
        if parsed["connection"] != self.cid:
            return None
        return parsed, raw

    def handshake(self, timeout=5.0):
        cid = self.cid.to_bytes(8, "big")
        private = X25519PrivateKey.generate()
        public = private.public_key().public_bytes(encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw)
        client_part = public + os.urandom(16)
        hello = client_part + mac(self.psk, b"MIRA-T/1 client hello" + cid + client_part)
        hello_packet = header(1, self.cid, 0, 0, len(hello)) + hello
        self.sock.sendto(hello_packet, self.addr)
        deadline = time.monotonic() + timeout
        ack = None
        while time.monotonic() < deadline:
            try:
                packet = self.packet()
            except socket.timeout:
                self.sock.sendto(hello_packet, self.addr)
                continue
            if packet is None:
                continue
            h, raw = packet
            if h["kind"] != 3 or h["number"] != 0:
                continue
            b = h["body"]
            if len(b) == 80 and hmac.compare_digest(b[48:], mac(self.psk, b"MIRA-T/1 server hello" + hello_packet + b[:48])):
                ack = b
                break
        if ack is None:
            raise TimeoutError("HELLO_ACK not received")
        shared = private.exchange(X25519PublicKey.from_public_bytes(ack[:32]))
        digest = hashlib.sha256(b"MIRA-T/1 transcript v3" + b"\x04" + cid + struct.pack("!I", len(client_part)) + client_part + struct.pack("!I", 48) + ack[:48]).digest()
        master = mac(self.psk, b"MIRA-T/1 traffic extract v3" + shared + digest)
        self.tx, self.iv_tx = derive(master, cid, True)
        self.rx, self.iv_rx = derive(master, cid, False)
        finish = b"MIRA-T/1 CLIENT FINISH"
        h = header(4, self.cid, 1, 0, len(finish) + 16)
        packet = h + AESGCM(self.tx).encrypt(nonce(self.iv_tx, 1), finish, h)
        self.sock.sendto(packet, self.addr)
        while time.monotonic() < deadline:
            try:
                result = self.packet()
            except socket.timeout:
                self.sock.sendto(packet, self.addr)
                continue
            if not result:
                continue
            p, raw = result
            if p["kind"] != 5 or p["number"] != 1:
                continue
            try:
                pt = AESGCM(self.rx).decrypt(nonce(self.iv_rx, 1), p["body"], raw[:26])
            except Exception:
                continue
            if (8, 0, b"READY") in frames(pt):
                return
        raise TimeoutError("encrypted READY not received")

    def events(self):
        result = self.packet()
        if result is None:
            return []
        p, raw = result
        if p["kind"] != 5 or p["number"] < 2 or p["epoch"] != 0:
            return []
        plain = AESGCM(self.rx).decrypt(nonce(self.iv_rx, p["number"]), p["body"], raw[:26])
        fs = frames(plain)
        # Acknowledge the server's reliable packet, without creating an ACK storm.
        if p["flags"] & 1:
            self.send(15, uvarint(1) + uvarint(p["number"]) + uvarint(p["number"]), False)
        return fs

    def exchange(self, message=b"hello-from-independent-python"):
        self.send(7, uvarint(1 << 20))  # server-to-client flow credit
        self.send(3, uvarint(1) + bytes([1, 0]) + uvarint(65536) + uvarint(0))
        opened = False
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            try:
                fs = self.events()
            except socket.timeout:
                continue
            for kind, flags, body in fs:
                if kind == 4:
                    cid, off = read_varint(body, 0)
                    if cid == 1:
                        opened = True
                if kind == 6:
                    raise RuntimeError("server rejected channel")
            if opened:
                break
        if not opened:
            raise TimeoutError("OPEN_ACK missing")
        body = uvarint(1) + uvarint(1) + uvarint(len(message)) + uvarint(0) + uvarint(900) + message
        self.send(1, body)
        while time.monotonic() < deadline:
            try:
                fs = self.events()
            except socket.timeout:
                continue
            for kind, flags, body in fs:
                if kind == 1:
                    cid, pos = read_varint(body, 0)
                    msg, pos = read_varint(body, pos)
                    total, pos = read_varint(body, pos)
                    offset, pos = read_varint(body, pos)
                    size, pos = read_varint(body, pos)
                    if cid == 1 and msg == 1 and offset == 0 and body[pos:] == b"echo: " + message:
                        return body[pos:]
                if kind == 6:
                    raise RuntimeError("server reset channel")
        raise TimeoutError("encrypted server response missing")

    def close(self):
        self.sock.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--server", default="127.0.0.1:7447")
    p.add_argument("--keyhex", default=os.environ.get("MIRA_PSK", ""))
    p.add_argument("--message", default="cross-language-test")
    args = p.parse_args()
    host, port = args.server.rsplit(":", 1)
    c = Client(host, int(port), bytes.fromhex(args.keyhex))
    try:
        c.handshake()
        reply = c.exchange(args.message.encode())
        print("Python independently negotiated encrypted MIRA, echo:", reply.decode())
    finally:
        c.close()


if __name__ == "__main__":
    main()
