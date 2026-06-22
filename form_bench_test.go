package fh

import (
	"testing"
)

func BenchmarkParseFormBytesSimple(b *testing.B) {
	data := []byte("name=John&age=30&city=New+York&country=US&active=true")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parseFormBytes(data)
	}
}

func BenchmarkParseFormBytesNested(b *testing.B) {
	data := []byte("user[name]=John&user[age]=30&user[address][city]=NYC&user[address][state]=NY&tags[]=go&tags[]=fast&tags[]=http")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parseFormBytes(data)
	}
}

func BenchmarkParseFormBytesEncoded(b *testing.B) {
	data := []byte("key=hello+world&path=%2Ftest%2Fpath&query=search+query&redirect=%2Ftarget%3Fparam%3Dvalue")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parseFormBytes(data)
	}
}

func BenchmarkURLDecodeNoEncoding(b *testing.B) {
	data := []byte("simple-key-without-encoding")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		urlDecode(data)
	}
}

func BenchmarkURLDecodeWithEncoding(b *testing.B) {
	data := []byte("hello+world+test+data+with+spaces")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		urlDecode(data)
	}
}

func BenchmarkURLDecodeWithPercent(b *testing.B) {
	data := []byte("%2Ftest%2Fpath%3Fparam%3Dvalue%26more%3Ddata")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		urlDecode(data)
	}
}

func BenchmarkDecodeFormSimple(b *testing.B) {
	var v map[string]any
	data := []byte("name=John&age=30&city=New+York")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v = nil
		_ = DecodeForm(data, &v)
	}
}

func BenchmarkDecodeFormNested(b *testing.B) {
	var v map[string]any
	data := []byte("user[name]=John&user[age]=30&address[city]=NYC&address[state]=NY")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v = nil
		_ = DecodeForm(data, &v)
	}
}

func BenchmarkParseQuery(b *testing.B) {
	c := &Ctx{
		queryParams: make([]Param, 0, 16),
	}
	c.Header.URI = []byte("/path?name=John&age=30&city=New+York&country=US&active=true")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.queryParsed = false
		c.qcount = 0
		c.queryParams = c.queryParams[:0]
		c.parseQuery()
	}
}
