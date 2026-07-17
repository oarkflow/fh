package kernel

import "net"

// Host connects the transport package to fh's HTTP lifecycle without creating
// an import cycle between the root package and this package.
type Host struct {
	StartServing          func(net.Listener) error
	FinishServing         func()
	AcceptConnection      func(net.Conn) bool
	SetRuntime            func(closer interface{ Close() error }, info KernelRuntimeInfo)
	PrintStartupBanner    func(net.Listener)
	BeginShutdown         func() error
	Closed                func() bool
	NormalizeServeError   func(error, bool) error
	LogInfo               func(string, ...any)
	LogWarn               func(string, ...any)
	AddAcceptErrors       func(uint64)
	AddPinnedThreads      func(int32)
	AddSocketOptionErrors func(uint64)
}

func (h Host) closed() bool { return h.Closed != nil && h.Closed() }
func (h Host) accept(c net.Conn) bool {
	return h.AcceptConnection != nil && h.AcceptConnection(c)
}
func (h Host) acceptError() {
	if h.AddAcceptErrors != nil {
		h.AddAcceptErrors(1)
	}
}
func (h Host) socketOptionErrors(n int) {
	if n > 0 && h.AddSocketOptionErrors != nil {
		h.AddSocketOptionErrors(uint64(n))
	}
}
func (h Host) pinned(delta int32) {
	if h.AddPinnedThreads != nil {
		h.AddPinnedThreads(delta)
	}
}
func (h Host) warn(msg string, args ...any) {
	if h.LogWarn != nil {
		h.LogWarn(msg, args...)
	}
}
func (h Host) info(msg string, args ...any) {
	if h.LogInfo != nil {
		h.LogInfo(msg, args...)
	}
}
func (h Host) normalize(err error) error {
	if h.NormalizeServeError != nil {
		return h.NormalizeServeError(err, h.closed())
	}
	return err
}
