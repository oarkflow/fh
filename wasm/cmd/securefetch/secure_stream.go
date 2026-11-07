//go:build js && wasm

package main

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"strings"
	"syscall/js"
	"time"

	protocol "github.com/oarkflow/fh/pkg/securetransport"
)

func (c *secureClient) requestStreamJS(_ js.Value, args []js.Value) any {
	return promise(func() (js.Value, error) {
		if len(args) == 0 || args[0].Type() != js.TypeObject {
			return js.Undefined(), errors.New("fh secure fetch: stream request descriptor is required")
		}
		return c.secureRequestStream(args[0])
	})
}

func (c *secureClient) secureRequestStream(v js.Value) (js.Value, error) {
	if err := c.establishSessionWithRecovery(); err != nil {
		return js.Undefined(), err
	}
	requestURL := stringValue(v.Get("url"))
	target := stringValue(v.Get("target"))
	method := strings.ToUpper(defaultString(stringValue(v.Get("method")), "GET"))
	if requestURL == "" || target == "" || !strings.HasPrefix(target, "/") {
		return js.Undefined(), errors.New("fh secure fetch: URL and target are required")
	}
	if err := c.validateDestination(requestURL); err != nil {
		return js.Undefined(), err
	}
	headers, contentType, err := headersFromJS(v.Get("headers"))
	if err != nil {
		return js.Undefined(), err
	}
	c.mu.Lock()
	session := c.session
	if session == nil {
		c.mu.Unlock()
		return js.Undefined(), errors.New("fh secure fetch: session is unavailable")
	}
	session.sequence++
	sequence := session.sequence
	sessionID := session.ID
	keys := session.Keys
	sessionExpiry := session.ExpiresAt
	cfg := c.cfg
	c.mu.Unlock()
	defer wipe(keys.ClientToServer[:])
	defer wipe(keys.ServerToClient[:])
	if len(cfg.ResponseSigningPublic) != 0 {
		return js.Undefined(), errors.New("fh secure fetch: signed-response policy requires whole-body mode")
	}
	now := time.Now()
	expires := now.Add(cfg.RequestTTL)
	if expires.After(sessionExpiry) {
		expires = sessionExpiry
	}
	requestID, err := protocol.NewID()
	if err != nil {
		return js.Undefined(), err
	}
	nonce, err := protocol.NewAEADNonce()
	if err != nil {
		return js.Undefined(), err
	}
	meta := protocol.StreamMetadata{SessionID: sessionID, RequestID: requestID, Sequence: sequence, IssuedAt: now.UnixMilli(), ExpiresAt: expires.UnixMilli(), Nonce: nonce}
	head, err := protocol.EncodeRequestStreamHead(protocol.RequestPayload{ContentType: contentType, Headers: headers}, cfg.Limits)
	if err != nil {
		return js.Undefined(), err
	}
	outerHeaders := js.Global().Get("Headers").New()
	outerHeaders.Call("set", protocol.HeaderSecure, "2")
	outerHeaders.Call("set", protocol.HeaderStream, base64.RawURLEncoding.EncodeToString(meta.Encode()))
	outerHeaders.Call("set", "Accept", protocol.MediaTypeStream)
	opts := js.Global().Get("Object").New()
	opts.Set("method", method)
	opts.Set("headers", outerHeaders)
	opts.Set("credentials", defaultString(stringValue(v.Get("credentials")), cfg.Credentials))
	opts.Set("cache", "no-store")
	opts.Set("redirect", "error")
	opts.Set("referrerPolicy", "no-referrer")
	if signal := v.Get("signal"); !signal.IsUndefined() && !signal.IsNull() {
		opts.Set("signal", signal)
	}
	if mode := stringValue(v.Get("mode")); mode != "" {
		if mode != "cors" && mode != "same-origin" {
			return js.Undefined(), errors.New("fh secure fetch: only cors and same-origin request modes are allowed")
		}
		opts.Set("mode", mode)
	}
	if method == "GET" || method == "HEAD" {
		var wire bytes.Buffer
		encoder, err := protocol.NewStreamEncoder(&wire, keys.ClientToServer, meta, protocol.StreamBinding{Method: method, Target: target}, cfg.Limits)
		if err != nil {
			return js.Undefined(), err
		}
		if err := encoder.WriteHead(head); err != nil {
			return js.Undefined(), err
		}
		if err := encoder.Close(); err != nil {
			return js.Undefined(), err
		}
		outerHeaders.Call("set", protocol.HeaderEnvelope, base64.RawURLEncoding.EncodeToString(wire.Bytes()))
	} else {
		outerHeaders.Call("set", "Content-Type", protocol.MediaTypeStream)
		uploadReader, uploadWriter := io.Pipe()
		encoder, err := protocol.NewStreamEncoder(uploadWriter, keys.ClientToServer, meta, protocol.StreamBinding{Method: method, Target: target}, cfg.Limits)
		if err != nil {
			return js.Undefined(), err
		}
		var source io.Reader = bytes.NewReader(nil)
		if bodyStream := v.Get("bodyStream"); !bodyStream.IsUndefined() && !bodyStream.IsNull() {
			streamReader, err := newJSReadableReader(bodyStream)
			if err != nil {
				return js.Undefined(), err
			}
			defer streamReader.Close()
			source = streamReader
		}
		go func() {
			if err := encoder.WriteHead(head); err != nil {
				_ = uploadWriter.CloseWithError(err)
				return
			}
			if _, err := io.CopyBuffer(encoder, source, make([]byte, protocol.StreamChunkSize)); err != nil {
				_ = uploadWriter.CloseWithError(err)
				return
			}
			_ = uploadWriter.CloseWithError(encoder.Close())
		}()
		defer uploadReader.Close()
		opts.Set("body", newJSReadableStream(uploadReader))
		opts.Set("duplex", "half")
	}
	responseValue, err := await(c.nativeFetch.Invoke(requestURL, opts))
	if err != nil {
		return js.Undefined(), err
	}
	status := responseValue.Get("status").Int()
	responseHeaders := responseValue.Get("headers")
	if responseHeaders.Call("get", protocol.HeaderSecure).String() != "2" {
		return js.Undefined(), errors.New("fh secure fetch: unprotected stream response")
	}
	outerContentType := strings.ToLower(strings.TrimSpace(strings.Split(responseHeaders.Call("get", "Content-Type").String(), ";")[0]))
	if outerContentType != protocol.MediaTypeStream {
		return js.Undefined(), errors.New("fh secure fetch: unexpected stream response content type")
	}
	metaHeader := responseHeaders.Call("get", protocol.HeaderStream)
	if metaHeader.IsNull() || metaHeader.String() == "" || len(metaHeader.String()) > 256 {
		return js.Undefined(), errors.New("fh secure fetch: response stream metadata is missing")
	}
	metaRaw, err := base64.RawURLEncoding.DecodeString(metaHeader.String())
	if err != nil {
		return js.Undefined(), err
	}
	responseMeta, err := protocol.DecodeStreamMetadata(metaRaw)
	if err != nil {
		return js.Undefined(), err
	}
	if !protocol.EqualID(responseMeta.SessionID, sessionID) || !protocol.EqualID(responseMeta.RequestID, requestID) || responseMeta.Sequence != sequence {
		return js.Undefined(), errors.New("fh secure fetch: stream response binding mismatch")
	}
	if err := protocol.ValidateTime(responseMeta.IssuedAt, responseMeta.ExpiresAt, time.Now(), cfg.ClockSkew); err != nil || responseMeta.ExpiresAt > sessionExpiry.Add(cfg.ClockSkew).UnixMilli() {
		return js.Undefined(), errors.New("fh secure fetch: response stream expired")
	}
	noBody := method == "HEAD" || status == 204 || status == 205 || status == 304 || (status >= 100 && status < 200)
	var wireReader io.Reader
	var browserReader *jsReadableReader
	if noBody {
		envelope := responseHeaders.Call("get", protocol.HeaderResponse)
		if envelope.IsNull() || envelope.String() == "" || len(envelope.String()) > 64<<10 {
			return js.Undefined(), errors.New("fh secure fetch: protected no-body response is missing")
		}
		wire, err := base64.RawURLEncoding.DecodeString(envelope.String())
		if err != nil {
			return js.Undefined(), err
		}
		wireReader = bytes.NewReader(wire)
	} else {
		browserReader, err = newJSReadableReader(responseValue.Get("body"))
		if err != nil {
			return js.Undefined(), err
		}
		wireReader = browserReader
	}
	decoder, err := protocol.NewStreamDecoder(wireReader, keys.ServerToClient, responseMeta, protocol.StreamBinding{Method: method, Target: target, Status: status, Response: true}, cfg.Limits)
	if err != nil {
		return js.Undefined(), err
	}
	responseHead, err := decoder.ReadHead()
	if err != nil {
		return js.Undefined(), err
	}
	payload, err := protocol.DecodeResponseStreamHead(responseHead, cfg.Limits)
	wipe(responseHead)
	if err != nil || payload.Status != status {
		return js.Undefined(), errors.New("fh secure fetch: protected response status mismatch")
	}
	resultHeaders := js.Global().Get("Array").New()
	for _, header := range payload.Headers {
		pair := js.Global().Get("Array").New()
		pair.Call("push", header.Name)
		pair.Call("push", header.Value)
		resultHeaders.Call("push", pair)
	}
	if payload.ContentType != "" {
		pair := js.Global().Get("Array").New()
		pair.Call("push", "content-type")
		pair.Call("push", payload.ContentType)
		resultHeaders.Call("push", pair)
	}
	result := js.Global().Get("Object").New()
	result.Set("status", status)
	result.Set("headers", resultHeaders)
	if noBody {
		body, err := io.ReadAll(decoder)
		if err != nil || len(body) != 0 {
			return js.Undefined(), errors.New("fh secure fetch: invalid protected no-body response")
		}
		result.Set("body", uint8Array(nil))
	} else {
		result.Set("body", newJSReadableStream(decoder))
	}
	result.Set("url", requestURL)
	result.Set("requestId", protocol.EncodeID(requestID))
	return result, nil
}
