package realip

import "testing"

func TestNewDefaultDoesNotPanic(t *testing.T) { _ = New(Config{}) }
