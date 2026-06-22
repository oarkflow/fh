package fh

import (
	"bytes"
	"testing"
)

type benchCodecUser struct {
	ID     int      `json:"id"`
	Name   string   `json:"name"`
	Email  string   `json:"email"`
	Active bool     `json:"active"`
	Roles  []string `json:"roles"`
}

var (
	benchCodecValue    = benchCodecUser{ID: 42, Name: "Alice", Email: "a@example.com", Active: true, Roles: []string{"admin", "owner"}}
	benchCodecJSON     = []byte(`{"id":42,"name":"Alice","email":"a@example.com","active":true,"roles":["admin","owner"]}`)
	benchCodecBytes    []byte
	benchCodecUserSink benchCodecUser
)

func BenchmarkCodecJSONMarshal(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out, err := EncodeBody("application/json", benchCodecValue)
		if err != nil {
			b.Fatal(err)
		}
		benchCodecBytes = out
	}
}

func BenchmarkCodecJSONUnmarshal(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var u benchCodecUser
		if err := DecodeBody(benchCodecJSON, "application/json", &u); err != nil {
			b.Fatal(err)
		}
		benchCodecUserSink = u
	}
}

func BenchmarkCodecJSONEncoder(b *testing.B) {
	b.ReportAllocs()
	var buf bytes.Buffer
	for i := 0; i < b.N; i++ {
		buf.Reset()
		if err := NewJSONEncoder(&buf).Encode(benchCodecValue); err != nil {
			b.Fatal(err)
		}
	}
	benchCodecBytes = buf.Bytes()
}
