package fh

import (
	"io"
)

// boundedRequestReader permits limit bytes and reports a 413 if more data is
// present. It reads the probe byte without passing it to the caller.
type boundedRequestReader struct {
	reader io.Reader
	left   int64
	err    error
}

func (r *boundedRequestReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r.left == 0 {
		var probe [1]byte
		n, err := r.reader.Read(probe[:])
		if n > 0 {
			r.err = PayloadTooLarge("request body exceeds configured limit")
			return 0, r.err
		}
		return 0, err
	}
	if int64(len(p)) > r.left {
		p = p[:int(r.left)]
	}
	n, err := r.reader.Read(p)
	r.left -= int64(n)
	return n, err
}

// LimitRequestBody installs a read-time limit for a request. The middleware
// can call it before the handler without buffering a streamed request body.
// Applications must enable WithStreamRequestBody(true) to avoid initial
// buffering. Custom Ctx implementations fall back to their Body method.
func LimitRequestBody(c Ctx, limit int64) error {
	if limit <= 0 {
		return nil
	}
	if dc, ok := c.(*DefaultCtx); ok {
		return dc.limitRequestBody(limit)
	}
	if int64(len(c.Body())) > limit {
		return PayloadTooLarge("request body exceeds configured limit")
	}
	return nil
}

func (c *DefaultCtx) limitRequestBody(limit int64) error {
	if c.Header.HasContentLength && int64(c.Header.ContentLength) > limit {
		return PayloadTooLarge("request body exceeds configured limit")
	}
	if c.requestBodyStream == nil && c.requestBodyReader == nil {
		if int64(len(c.body)) > limit {
			return PayloadTooLarge("request body exceeds configured limit")
		}
		return nil
	}
	var reader io.Reader
	if c.requestBodyLimit != nil {
		reader = c.requestBodyLimit
	} else if c.requestBodyStream != nil {
		reader = c.requestBodyStream
	} else {
		reader = c.requestBodyReader
	}
	c.requestBodyLimit = &boundedRequestReader{reader: reader, left: limit}
	return nil
}

// RequestBodyLimitError reports a read-time limit violation after a handler
// returns, including when that handler ignored an error from Body().
func RequestBodyLimitError(c Ctx) error {
	dc, ok := c.(*DefaultCtx)
	if !ok {
		return nil
	}
	for r := dc.requestBodyLimit; r != nil; {
		if r.err != nil {
			return r.err
		}
		parent, ok := r.reader.(*boundedRequestReader)
		if !ok {
			break
		}
		r = parent
	}
	return nil
}
