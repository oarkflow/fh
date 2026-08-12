package fh

// ProblemDetailsPayload represents an RFC 9457 / RFC 7807 problem details object.
type ProblemDetailsPayload struct {
	Type     string         `json:"type,omitempty"`
	Title    string         `json:"title"`
	Status   int            `json:"status"`
	Detail   string         `json:"detail,omitempty"`
	Instance string         `json:"instance,omitempty"`
	Extra    map[string]any `json:"extra,omitempty"`
}

// ProblemDetails sends an RFC 9457 / RFC 7807 application/problem+json error response.
func ProblemDetails(c Ctx, status int, title, detail, typeURI string) error {
	if typeURI == "" {
		typeURI = "about:blank"
	}
	if title == "" {
		title = StatusReason(status)
	}
	p := ProblemDetailsPayload{
		Type:   typeURI,
		Title:  title,
		Status: status,
		Detail: detail,
	}
	c.Status(status)
	c.Type("application/problem+json")
	return c.JSON(p)
}
