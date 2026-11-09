// Package replay tracks packet numbers to reject replayed authenticated packets.
// A window is owned by one session actor; it is not concurrently safe.
package replay

// Window provides a 1024-packet sliding replay bitmap. CheckAndMark MUST be
// called only after the packet AEAD authenticates successfully.
type Window struct {
	highest     uint64
	bits        [16]uint64
	initialized bool
}

// CheckAndMark returns true for an unseen number within the window.
func (w *Window) CheckAndMark(number uint64) bool {
	if !w.initialized {
		w.highest, w.initialized = number, true
		w.bits[0] = 1
		return true
	}
	if number > w.highest {
		shift := number - w.highest
		if shift >= 1024 {
			w.bits = [16]uint64{}
		} else {
			words := int(shift / 64)
			rem := uint(shift % 64)
			if words > 0 {
				for i := 15; i >= 0; i-- {
					if i >= words {
						w.bits[i] = w.bits[i-words]
					} else {
						w.bits[i] = 0
					}
				}
			}
			if rem > 0 {
				for i := 15; i >= 0; i-- {
					v := w.bits[i] << rem
					if i > 0 {
						v |= w.bits[i-1] >> (64 - rem)
					}
					w.bits[i] = v
				}
			}
		}
		w.highest = number
		w.bits[0] |= 1
		return true
	}
	age := w.highest - number
	if age >= 1024 {
		return false
	}
	idx, mask := int(age/64), uint64(1)<<uint(age%64)
	if w.bits[idx]&mask != 0 {
		return false
	}
	w.bits[idx] |= mask
	return true
}

// Seen reports whether a packet number is recorded in the replay bitmap.
// Too-old packet numbers are NOT reported as duplicates; they must be dropped.
func (w *Window) Seen(number uint64) bool {
	if !w.initialized {
		return false
	}
	if number > w.highest {
		return false
	}
	age := w.highest - number
	if age >= 1024 {
		return false
	}
	idx, mask := int(age/64), uint64(1)<<uint(age%64)
	return w.bits[idx]&mask != 0
}

// TooOld reports a packet older than the retained replay bitmap. Such packets
// must be dropped without ACK: the receiver cannot know whether processed.
func (w *Window) TooOld(number uint64) bool {
	return w.initialized && number <= w.highest && w.highest-number >= 1024
}
