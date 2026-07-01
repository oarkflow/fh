package bulkhead

import "testing"

func TestNewDefaultDoesNotPanic(t *testing.T) { _ = New(Config{MaxConcurrent: 1}) }
