package durable

import "crypto/sha256"

func sum256(b []byte) [32]byte { return sha256.Sum256(b) }
