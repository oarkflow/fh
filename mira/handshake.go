package mira

import (
	"bytes"
	"crypto/ecdh"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/oarkflow/fh/internal/cryptokit"
	"github.com/oarkflow/fh/internal/wire"
)

// HELLO: X25519 ephemeral public key (32), random nonce (16), PSK HMAC (32).
// HELLO_ACK: server X25519 public (32), server nonce (16), PSK transcript HMAC (32).
// Key schedule mixes PSK, ephemeral DH, both nonces, and both public keys.
// This is a bespoke PSK authenticated handshake, not Noise/TLS: independent
// cryptographic analysis is mandatory before handling sensitive traffic.
const helloSize = 80

func randomBytes(b []byte) error { _, err := rand.Read(b); return err }

func newEphemeral() (*ecdh.PrivateKey, error) { return ecdh.X25519().GenerateKey(rand.Reader) }

func makeMAC(psk []byte, label string, parts ...[]byte) []byte {
	m := hmac.New(sha256.New, psk)
	_, _ = m.Write([]byte("MIRA-T/1 "))
	_, _ = m.Write([]byte(label))
	for _, part := range parts {
		_, _ = m.Write(part)
	}
	return m.Sum(nil)
}

// parseHelloBody accepts either a legacy single-PSK HELLO or a HELLO with
// a named credential. Identity is authenticated by the client's HMAC.
func parseHelloBody(p []byte) (identity string, clientPart, public []byte, ok bool) {
	if len(p) == helloSize {
		return "", p[:48], p[:32], true
	}
	if len(p) < helloSize+2 || p[0] == 0 || p[0] > 64 || len(p) != 1+int(p[0])+helloSize {
		return "", nil, nil, false
	}
	identity = string(p[1 : 1+int(p[0])])
	for _, r := range identity {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' || r == '@') {
			return "", nil, nil, false
		}
	}
	part := p[:len(p)-32]
	pubStart := 1 + int(p[0])
	return identity, part, p[pubStart : pubStart+32], true
}

func makeHello(psk []byte, id uint64, key *ecdh.PrivateKey, nonce [16]byte, identity string) ([]byte, error) {
	var content []byte
	if identity != "" {
		if len(identity) > 64 {
			return nil, errors.New("mira: identity exceeds 64 bytes")
		}
		for _, r := range identity {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' || r == '@') {
				return nil, errors.New("mira: invalid identity")
			}
		}
		content = append([]byte{byte(len(identity))}, []byte(identity)...)
	}
	content = append(content, key.PublicKey().Bytes()...)
	content = append(content, nonce[:]...)
	var cid [8]byte
	binary.BigEndian.PutUint64(cid[:], id)
	content = append(content, makeMAC(psk, "client hello", cid[:], content)...)
	return packetPlain(wire.KindHello, id, 0, content)
}

func checkHello(psk, p []byte, id uint64) bool {
	_, part, _, ok := parseHelloBody(p)
	if !ok || id == 0 || len(psk) < 32 {
		return false
	}
	var cid [8]byte
	binary.BigEndian.PutUint64(cid[:], id)
	return hmac.Equal(p[len(part):], makeMAC(psk, "client hello", cid[:], part))
}

func makeHelloAck(psk, hello []byte, id uint64, key *ecdh.PrivateKey, nonce [16]byte) ([]byte, error) {
	var body [helloSize]byte
	copy(body[:32], key.PublicKey().Bytes())
	copy(body[32:48], nonce[:])
	copy(body[48:], makeMAC(psk, "server hello", hello, body[:48]))
	return packetPlain(wire.KindHelloAck, id, 0, body[:])
}

func checkHelloAck(psk, hello, ack []byte) bool {
	if len(ack) != helloSize {
		return false
	}
	return hmac.Equal(ack[48:], makeMAC(psk, "server hello", hello, ack[:48]))
}

func packetPlain(kind uint8, cid, pn uint64, body []byte) ([]byte, error) {
	hdr := wire.Header{Kind: kind, Connection: cid, Number: pn, Length: uint16(len(body))}
	raw := make([]byte, wire.HeaderSize+len(body))
	if err := hdr.MarshalTo(raw); err != nil {
		return nil, err
	}
	copy(raw[wire.HeaderSize:], body)
	return raw, nil
}

// trafficSchedule binds per-epoch keys to the authenticated ephemeral handshake.
// Key epochs are derived deterministically from packet numbers. This rotates
// packet AEAD keys but is NOT a forward-secret ratchet within one session.
type trafficSchedule struct {
	master [32]byte
	id     uint64
	client bool
}

const keyEpochPackets uint64 = 4096

// initialChain derives independent directional hash chains. Advancing a
// chain with HMAC is one-way; the previous chain secret is cleared on rollover.
// This provides past-epoch key erasure but NOT post-compromise recovery.
func (k trafficSchedule) initialChain(outbound bool) [32]byte {
	direction := "c2s"
	if k.client != outbound {
		direction = "s2c"
	}
	var cid [8]byte
	binary.BigEndian.PutUint64(cid[:], k.id)
	mac := hmac.New(sha256.New, k.master[:])
	mac.Write([]byte("MIRA-T/1 v4 directional chain "))
	mac.Write([]byte(direction))
	mac.Write(cid[:])
	var result [32]byte
	copy(result[:], mac.Sum(nil))
	return result
}
func advanceChain(previous [32]byte, epoch uint16) [32]byte {
	var e [2]byte
	binary.BigEndian.PutUint16(e[:], epoch)
	mac := hmac.New(sha256.New, previous[:])
	mac.Write([]byte("MIRA-T/1 v4 irreversible epoch step"))
	mac.Write(e[:])
	var result [32]byte
	copy(result[:], mac.Sum(nil))
	return result
}
func (k trafficSchedule) fromChain(chain [32]byte, epoch uint16, outbound bool) (*cryptokit.Protector, error) {
	var idAndEpoch [10]byte
	binary.BigEndian.PutUint64(idAndEpoch[:8], k.id)
	binary.BigEndian.PutUint16(idAndEpoch[8:], epoch)
	direction := "c2s:"
	if k.client != outbound {
		direction = "s2c:"
	}
	context := append([]byte("v4 "+direction), idAndEpoch[:]...)
	keys, err := cryptokit.DeriveTrafficKeys(chain[:], context)
	if err != nil {
		return nil, err
	}
	return cryptokit.NewProtector(keys)
}
func (k trafficSchedule) protector(epoch uint16, outbound bool) (*cryptokit.Protector, error) {
	chain := k.initialChain(outbound)
	for i := 1; i <= int(epoch); i++ {
		next := advanceChain(chain, uint16(i))
		clear(chain[:])
		chain = next
	}
	p, err := k.fromChain(chain, epoch, outbound)
	clear(chain[:])
	return p, err
}

func derive(psk []byte, key *ecdh.PrivateKey, peer []byte, clientPart, serverPart []byte, cid uint64, client bool) (tx, rx *cryptokit.Protector, schedule trafficSchedule, err error) {
	pub, err := ecdh.X25519().NewPublicKey(peer)
	if err != nil {
		return nil, nil, schedule, err
	}
	shared, err := key.ECDH(pub)
	if err != nil {
		return nil, nil, schedule, err
	}
	// Transcript is length-delimited to avoid concat ambiguity and includes
	// the major wire revision, both ephemeral keys, nonces, credential id and
	// CID. HMAC extracts a master from both the PSK and ephemeral DH output.
	// This remains a bespoke PSK handshake: independent analysis is required.
	transcript := sha256.New()
	transcript.Write([]byte("MIRA-T/1 transcript v3"))
	transcript.Write([]byte{wire.Version})
	var id [8]byte
	binary.BigEndian.PutUint64(id[:], cid)
	transcript.Write(id[:])
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(clientPart)))
	transcript.Write(length[:])
	transcript.Write(clientPart)
	binary.BigEndian.PutUint32(length[:], uint32(len(serverPart)))
	transcript.Write(length[:])
	transcript.Write(serverPart)
	masterMAC := hmac.New(sha256.New, psk)
	masterMAC.Write([]byte("MIRA-T/1 traffic extract v3"))
	masterMAC.Write(shared)
	masterMAC.Write(transcript.Sum(nil))
	copy(schedule.master[:], masterMAC.Sum(nil))
	schedule.id = cid
	schedule.client = client
	tx, err = schedule.protector(0, true)
	if err != nil {
		return nil, nil, schedule, err
	}
	rx, err = schedule.protector(0, false)
	if err != nil {
		return nil, nil, schedule, err
	}
	return tx, rx, schedule, nil
}

func sealed(protector *cryptokit.Protector, cid, pn uint64, kind, flags uint8, frame []byte) ([]byte, error) {
	return sealedEpoch(protector, cid, pn, kind, flags, 0, frame)
}

func sealedEpoch(protector *cryptokit.Protector, cid, pn uint64, kind, flags uint8, epoch uint16, frame []byte) ([]byte, error) {
	hdr := wire.Header{Kind: kind, Flags: flags, Connection: cid, Number: pn, Epoch: epoch, Length: uint16(len(frame) + wire.AuthTagSize)}
	h, err := wire.HeaderBytes(hdr)
	if err != nil {
		return nil, err
	}
	raw := make([]byte, 0, wire.HeaderSize+int(hdr.Length))
	raw = append(raw, h[:]...)
	raw = protector.Seal(raw, frame, h[:], pn)
	if len(raw) > wire.MaxDatagram {
		return nil, errors.New("mira: packet exceeds path MTU")
	}
	return raw, nil
}

func opened(protector *cryptokit.Protector, packet []byte, kind uint8) (wire.Header, []byte, error) {
	h, err := wire.ParseHeader(packet)
	if err != nil {
		return h, nil, err
	}
	if h.Kind != kind || h.Epoch != 0 {
		return h, nil, wire.ErrInvalid
	}
	body, err := protector.Open(nil, packet[wire.HeaderSize:], packet[:wire.HeaderSize], h.Number)
	return h, body, err
}

func sameHello(old, new []byte) bool { return bytes.Equal(old, new) }

func openedEpoch(protector *cryptokit.Protector, packet []byte, kind uint8) (wire.Header, []byte, error) {
	h, err := wire.ParseHeader(packet)
	if err != nil {
		return h, nil, err
	}
	if h.Kind != kind {
		return h, nil, wire.ErrInvalid
	}
	body, err := protector.Open(nil, packet[wire.HeaderSize:], packet[:wire.HeaderSize], h.Number)
	return h, body, err
}
