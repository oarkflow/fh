package fh

import "strings"

// RequestPreference reports whether the Prefer request field contains the
// named preference, ignoring optional parameters and casing.
func (c *DefaultCtx) RequestPreference(name string) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	for item := range strings.SplitSeq(string(c.Header.Peek(HeaderPreferBytes)), ",") {
		item = strings.TrimSpace(item)
		if semi := strings.IndexByte(item, ';'); semi >= 0 {
			item = item[:semi]
		}
		if equal := strings.IndexByte(item, '='); equal >= 0 {
			item = item[:equal]
		}
		if strings.EqualFold(strings.TrimSpace(item), name) {
			return true
		}
	}
	return false
}

// SetPreferenceApplied records the preference applied by the response.
func (c *DefaultCtx) SetPreferenceApplied(value string) Ctx {
	if value != "" && !strings.ContainsAny(value, "\x00\r\n") {
		c.Set(HeaderPreferenceApplied, value)
	}
	return c
}
