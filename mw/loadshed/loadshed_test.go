package loadshed

import "testing"

func TestNewDefaultDoesNotPanic(t *testing.T) { _ = New(Config{MaxInFlight: 100}) }
