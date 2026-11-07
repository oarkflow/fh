package securetransport

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/oarkflow/fh"
	protocol "github.com/oarkflow/fh/pkg/securetransport"
)

func (t *Transport) handleSecureStream(c fh.Ctx) error {
	encoded := c.Get(protocol.HeaderStream)
	if len(encoded) == 0 || len(encoded) > 256 {
		return t.authFailure()
	}
	metadataBytes, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return t.authFailure()
	}
	meta, err := protocol.DecodeStreamMetadata(metadataBytes)
	if err != nil || meta.Sequence == 0 {
		return t.authFailure()
	}
	session, err := getSession(t.cfg.SessionStore, meta.SessionID)
	if err != nil || !session.ExpiresAt.After(time.Now()) {
		return t.authFailure()
	}
	defer zeroSession(&session)
	if err := protocol.ValidateTime(meta.IssuedAt, meta.ExpiresAt, time.Now(), t.cfg.MaxClockSkew); err != nil {
		return t.authFailure()
	}
	if time.UnixMilli(meta.ExpiresAt).Sub(time.UnixMilli(meta.IssuedAt)) > t.cfg.RequestTTL+t.cfg.MaxClockSkew || meta.ExpiresAt > session.ExpiresAt.UnixMilli()+t.cfg.MaxClockSkew.Milliseconds() {
		return t.authFailure()
	}
	if !t.cfg.SkipDeviceStatusCheck {
		if _, err := getDevice(t.cfg.DeviceStore, session.DeviceID); err != nil {
			return t.authFailure()
		}
	}
	read := func(raw io.Reader) error {
		decoder, err := protocol.NewStreamDecoder(raw, session.Keys.ClientToServer, meta, protocol.StreamBinding{Method: c.Method(), Target: c.OriginalURL()}, t.cfg.Limits)
		if err != nil {
			return t.authFailure()
		}
		head, err := decoder.ReadHead()
		if err != nil {
			return t.authFailure()
		}
		payload, err := protocol.DecodeRequestStreamHead(head, t.cfg.Limits)
		clear(head)
		if err != nil {
			return t.authFailure()
		}
		replayExpiry := time.UnixMilli(meta.ExpiresAt).Add(t.cfg.MaxClockSkew)
		sequenceKey := "request-sequence:" + protocol.EncodeID(session.ID) + ":" + strconv.FormatUint(meta.Sequence, 10)
		accepted, err := checkAndStoreReplay(t.cfg.ReplayStore, &t.replayMu, t.cfg.MaxReplayEntries, sequenceKey, replayExpiry)
		if err != nil {
			return fh.NewHTTPError(fh.StatusServiceUnavailable, "FH_SECURE_REPLAY_STORE", "secure request could not be accepted")
		}
		if !accepted {
			return fh.NewHTTPError(fh.StatusConflict, "FH_SECURE_REPLAY", "request replay detected")
		}
		requestIDKey := "request-id:" + protocol.EncodeID(session.ID) + ":" + protocol.EncodeID(meta.RequestID)
		accepted, err = checkAndStoreReplay(t.cfg.ReplayStore, &t.replayMu, t.cfg.MaxReplayEntries, requestIDKey, replayExpiry)
		if err != nil {
			return fh.NewHTTPError(fh.StatusServiceUnavailable, "FH_SECURE_REPLAY_STORE", "secure request could not be accepted")
		}
		if !accepted {
			return fh.NewHTTPError(fh.StatusConflict, "FH_SECURE_REPLAY", "request replay detected")
		}
		t.sanitizeOuterRequestHeaders(c)
		if !fh.ReplaceRequestBodyReader(c, decoder) {
			return errors.New("secure transport: request stream replacement is unavailable")
		}
		h := c.RequestHeader()
		h.HasContentLength = false
		h.ContentLength = 0
		h.Chunked = false
		if payload.ContentType != "" {
			h.Set(fh.HeaderContentTypeStr, payload.ContentType)
		}
		for _, header := range payload.Headers {
			if protocol.ValidProtectedHeader(header.Name, header.Value) {
				h.Set(header.Name, header.Value)
			}
		}
		h.Del(protocol.HeaderStream)
		h.Del(protocol.HeaderEnvelope)
		info := publicSession(session)
		c.Locals(LocalSession, info)
		c.Locals(LocalDevice, session.DeviceID)
		c.Locals(LocalRequest, meta.RequestID)
		_ = touchDevice(t.cfg.DeviceStore, session.DeviceID, time.Now())
		if t.cfg.ValidateSession != nil {
			if err := t.cfg.ValidateSession(c, info); err != nil {
				return err
			}
		}
		verifyEnd := func(fh.Ctx) error {
			_, err := io.Copy(io.Discard, decoder)
			if err != nil {
				return t.authFailure()
			}
			return nil
		}
		c.OnBeforeResponse(verifyEnd)
		t.installStreamResponseProtection(c, session, meta)
		handlerErr := c.Next()
		if err := verifyEnd(c); err != nil {
			return err
		}
		return handlerErr
	}
	if c.Method() == fh.MethodGETStr || c.Method() == fh.MethodHEADStr {
		wireHeader := c.Get(protocol.HeaderEnvelope)
		if wireHeader == "" || len(wireHeader) > 64<<10 {
			return t.authFailure()
		}
		wire, err := base64.RawURLEncoding.DecodeString(wireHeader)
		if err != nil {
			return t.authFailure()
		}
		return read(bytes.NewReader(wire))
	}
	return c.StreamBody(read)
}

func (t *Transport) installStreamResponseProtection(c fh.Ctx, session Session, request protocol.StreamMetadata) {
	var response protocol.StreamMetadata
	var headers []protocol.Header
	var contentType string
	c.OnBeforeResponse(func(c fh.Ctx) error {
		status := c.StatusCode()
		contentType = c.GetRespHeader(fh.HeaderContentTypeStr)
		var err error
		headers, err = t.collectResponseHeaders(c)
		if err != nil {
			return err
		}
		now := time.Now()
		expires := now.Add(t.cfg.RequestTTL)
		if expires.After(session.ExpiresAt) {
			expires = session.ExpiresAt
		}
		if !expires.After(now) {
			return errors.New("secure transport: session expired before response")
		}
		nonce, err := protocol.NewAEADNonce()
		if err != nil {
			return err
		}
		response = protocol.StreamMetadata{SessionID: session.ID, RequestID: request.RequestID, Sequence: request.Sequence, IssuedAt: now.UnixMilli(), ExpiresAt: expires.UnixMilli(), Nonce: nonce}
		if t.cfg.HideResponseHeaders {
			t.hideResponseHeaders(c)
		}
		c.Set(protocol.HeaderSecure, "2")
		c.Set(protocol.HeaderStream, base64.RawURLEncoding.EncodeToString(response.Encode()))
		c.Set("Cache-Control", "no-store")
		c.Set("Pragma", "no-cache")
		c.Set("X-Content-Type-Options", "nosniff")
		c.Append("Vary", protocol.HeaderSecure)
		c.Type(protocol.MediaTypeStream)
		if noStreamResponseBody(c.Method(), status) {
			wire, err := t.encodeStreamResponse(nil, c, session, response, headers, contentType)
			if err != nil {
				return err
			}
			if len(wire) > 32<<10 {
				return protocol.ErrTooLarge
			}
			c.Set(protocol.HeaderResponse, base64.RawURLEncoding.EncodeToString(wire))
		}
		return nil
	})
	c.AddBodyTransform(func(body []byte) ([]byte, error) {
		if noStreamResponseBody(c.Method(), c.StatusCode()) {
			return nil, nil
		}
		return t.encodeStreamResponse(body, c, session, response, headers, contentType)
	})
	fh.SetResponseStreamTransform(c, func(out io.Writer) (io.WriteCloser, error) {
		encoder, err := protocol.NewStreamEncoder(out, session.Keys.ServerToClient, response, protocol.StreamBinding{Method: c.Method(), Target: c.OriginalURL(), Status: c.StatusCode(), Response: true}, t.cfg.Limits)
		if err != nil {
			return nil, err
		}
		head, err := protocol.EncodeResponseStreamHead(protocol.ResponsePayload{Status: c.StatusCode(), ContentType: contentType, Headers: headers}, t.cfg.Limits)
		if err != nil {
			return nil, err
		}
		if err := encoder.WriteHead(head); err != nil {
			return nil, err
		}
		return encoder, nil
	})
}

func (t *Transport) encodeStreamResponse(body []byte, c fh.Ctx, session Session, meta protocol.StreamMetadata, headers []protocol.Header, contentType string) ([]byte, error) {
	var wire bytes.Buffer
	encoder, err := protocol.NewStreamEncoder(&wire, session.Keys.ServerToClient, meta, protocol.StreamBinding{Method: c.Method(), Target: c.OriginalURL(), Status: c.StatusCode(), Response: true}, t.cfg.Limits)
	if err != nil {
		return nil, err
	}
	head, err := protocol.EncodeResponseStreamHead(protocol.ResponsePayload{Status: c.StatusCode(), ContentType: contentType, Headers: headers}, t.cfg.Limits)
	if err != nil {
		return nil, err
	}
	if err := encoder.WriteHead(head); err != nil {
		return nil, err
	}
	if _, err := encoder.Write(body); err != nil {
		return nil, err
	}
	if err := encoder.Close(); err != nil {
		return nil, err
	}
	return wire.Bytes(), nil
}

func noStreamResponseBody(method string, status int) bool {
	return method == fh.MethodHEADStr || status == fh.StatusNoContent || status == fh.StatusResetContent || status == fh.StatusNotModified || (status >= 100 && status < 200)
}
