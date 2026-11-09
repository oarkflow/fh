# MIRA-T/1 version 4 framing fixtures

`hello_header_v4.hex` is a synthetic **26-byte pre-authentication header only**, not a valid authenticated UDP handshake. `ack_ranges_frame_v4.hex` is an unauthenticated frame fixture (ACK range [2,5]), not an encrypted packet. The wire format and old v3 fixtures deliberately differ; v3 is not compatible with v4.

The independent Python 3 standard-library parser `mira_wire_v4.py` tests canonical encoding and bounds for the packet header and frame TLV; it does **not** implement the cryptographic handshake or transport runtime.

```sh
python3 -m unittest discover -s conformance -p 'test_wire_v4.py' -v
go test ./internal/wire -run TestConformanceFixtures -count=1
```
