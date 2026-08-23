package fh

import "strconv"

// HTTPPriority is the RFC 9218 priority of a request or response.
// Urgency is in the inclusive range 0 (most urgent) through 7 (least urgent).
type HTTPPriority struct {
	Urgency     uint8
	Incremental bool
}

// RequestPriority returns the parsed Priority request field. Invalid or
// missing parameters use RFC 9218's defaults.
func (c *DefaultCtx) RequestPriority() HTTPPriority {
	priority := HTTPPriority{Urgency: 3}
	value := c.Header.Peek(HeaderPriorityBytes)
	for len(value) != 0 {
		for len(value) != 0 && (value[0] == ' ' || value[0] == '\t' || value[0] == ',') {
			value = value[1:]
		}
		if len(value) == 0 {
			break
		}
		end := 0
		for end < len(value) && value[end] != ',' {
			end++
		}
		item := value[:end]
		if len(item) >= 2 && (item[0] == 'u' || item[0] == 'U') && item[1] == '=' {
			if urgency, err := strconv.ParseUint(string(item[2:]), 10, 8); err == nil && urgency <= 7 {
				priority.Urgency = uint8(urgency)
			}
		} else if len(item) == 1 && (item[0] == 'i' || item[0] == 'I') {
			priority.Incremental = true
		}
		if end == len(value) {
			break
		}
		value = value[end+1:]
	}
	return priority
}

// SetResponsePriority sets a validated RFC 9218 Priority response field.
func (c *DefaultCtx) SetResponsePriority(priority HTTPPriority) Ctx {
	if priority.Urgency > 7 {
		priority.Urgency = 7
	}
	value := "u=" + strconv.Itoa(int(priority.Urgency))
	if priority.Incremental {
		value += ", i"
	}
	c.Set(HeaderPriority, value)
	return c
}
