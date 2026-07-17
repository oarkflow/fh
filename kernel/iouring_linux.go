//go:build linux

package kernel

import (
	"errors"
	"fmt"
	"net"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

const (
	linuxSYSIOUringSetup    = 425
	linuxSYSIOUringEnter    = 426
	linuxSYSIOUringRegister = 427

	ioUringOffSQRing = 0
	ioUringOffCQRing = 0x08000000
	ioUringOffSQEs   = 0x10000000

	ioUringFeatSingleMMap = 1 << 0
	ioUringSetupSubmitAll = 1 << 7

	ioUringOpAccept        = 13
	ioUringRegisterProbe   = 8
	ioUringOpSupported     = 1 << 0
	ioUringAcceptMultishot = 1 << 0
	ioUringCQEMore         = 1 << 1
	ioUringAcceptUserData  = 1
)

type ioUringSQOffsets struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Flags       uint32
	Dropped     uint32
	Array       uint32
	Resv1       uint32
	UserAddr    uint64
}

type ioUringCQOffsets struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Overflow    uint32
	CQEs        uint32
	Flags       uint32
	Resv1       uint32
	UserAddr    uint64
}

type ioUringParams struct {
	SQEntries    uint32
	CQEntries    uint32
	Flags        uint32
	SQThreadCPU  uint32
	SQThreadIdle uint32
	Features     uint32
	WQFD         uint32
	Resv         [3]uint32
	SQOff        ioUringSQOffsets
	CQOff        ioUringCQOffsets
}

type ioUringSQE struct {
	Opcode      uint8
	Flags       uint8
	IOPrio      uint16
	FD          int32
	Off         uint64
	Addr        uint64
	Len         uint32
	OpFlags     uint32
	UserData    uint64
	BufIndex    uint16
	Personality uint16
	SpliceFDIn  int32
	Addr3       uint64
	Pad2        uint64
}

type ioUringCQE struct {
	UserData uint64
	Res      int32
	Flags    uint32
}

type ioUringProbe struct {
	LastOp uint8
	OpsLen uint8
	Resv   uint16
	Resv2  [3]uint32
}

type ioUringProbeOp struct {
	Op    uint8
	Resv  uint8
	Flags uint16
	Resv2 uint32
}

type ioUring struct {
	fd        int
	params    ioUringParams
	sqRing    []byte
	cqRing    []byte
	sqesMap   []byte
	singleMap bool

	sqHead    *uint32
	sqTail    *uint32
	sqMask    *uint32
	sqEntries *uint32
	sqArray   []uint32
	sqes      []ioUringSQE

	cqHead    *uint32
	cqTail    *uint32
	cqMask    *uint32
	cqEntries *uint32
	cqes      []ioUringCQE
}

func ioUringSupportedArch() bool {
	switch runtime.GOARCH {
	case "amd64", "arm64", "386", "riscv64", "loong64", "ppc64", "ppc64le", "s390x":
		return true
	default:
		return false
	}
}

func setupIOUring(entries uint32) (*ioUring, error) {
	if !ioUringSupportedArch() {
		return nil, fmt.Errorf("fh: io_uring syscall number is unsupported on %s", runtime.GOARCH)
	}
	params := ioUringParams{Flags: ioUringSetupSubmitAll}
	fd, _, errno := syscall.RawSyscall(linuxSYSIOUringSetup, uintptr(entries), uintptr(unsafe.Pointer(&params)), 0)
	if errno == syscall.EINVAL {
		params = ioUringParams{}
		fd, _, errno = syscall.RawSyscall(linuxSYSIOUringSetup, uintptr(entries), uintptr(unsafe.Pointer(&params)), 0)
	}
	if errno != 0 {
		return nil, errno
	}
	ring := &ioUring{fd: int(fd), params: params}
	if err := ring.mapRings(); err != nil {
		_ = syscall.Close(ring.fd)
		return nil, err
	}
	return ring, nil
}

func (r *ioUring) mapRings() error {
	sqSize := int(r.params.SQOff.Array + r.params.SQEntries*4)
	cqSize := int(r.params.CQOff.CQEs + r.params.CQEntries*uint32(unsafe.Sizeof(ioUringCQE{})))
	var err error
	if r.params.Features&ioUringFeatSingleMMap != 0 {
		size := sqSize
		if cqSize > size {
			size = cqSize
		}
		r.sqRing, err = mmapIOUring(r.fd, ioUringOffSQRing, size)
		if err != nil {
			return err
		}
		r.cqRing = r.sqRing
		r.singleMap = true
	} else {
		r.sqRing, err = mmapIOUring(r.fd, ioUringOffSQRing, sqSize)
		if err != nil {
			return err
		}
		r.cqRing, err = mmapIOUring(r.fd, ioUringOffCQRing, cqSize)
		if err != nil {
			_ = syscall.Munmap(r.sqRing)
			return err
		}
	}
	r.sqesMap, err = mmapIOUring(r.fd, ioUringOffSQEs, int(r.params.SQEntries)*int(unsafe.Sizeof(ioUringSQE{})))
	if err != nil {
		r.unmapRings()
		return err
	}

	r.sqHead = ptrUint32(r.sqRing, r.params.SQOff.Head)
	r.sqTail = ptrUint32(r.sqRing, r.params.SQOff.Tail)
	r.sqMask = ptrUint32(r.sqRing, r.params.SQOff.RingMask)
	r.sqEntries = ptrUint32(r.sqRing, r.params.SQOff.RingEntries)
	r.sqArray = unsafe.Slice((*uint32)(unsafe.Pointer(&r.sqRing[r.params.SQOff.Array])), int(r.params.SQEntries))
	r.sqes = unsafe.Slice((*ioUringSQE)(unsafe.Pointer(&r.sqesMap[0])), int(r.params.SQEntries))

	r.cqHead = ptrUint32(r.cqRing, r.params.CQOff.Head)
	r.cqTail = ptrUint32(r.cqRing, r.params.CQOff.Tail)
	r.cqMask = ptrUint32(r.cqRing, r.params.CQOff.RingMask)
	r.cqEntries = ptrUint32(r.cqRing, r.params.CQOff.RingEntries)
	r.cqes = unsafe.Slice((*ioUringCQE)(unsafe.Pointer(&r.cqRing[r.params.CQOff.CQEs])), int(r.params.CQEntries))
	return nil
}

func mmapIOUring(fd, offset, size int) ([]byte, error) {
	b, err := syscall.Mmap(fd, int64(offset), size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE)
	if err == nil {
		return b, nil
	}
	// MAP_POPULATE is an optimization, not a correctness requirement. Some
	// kernels, emulators and memory policies reject it for io_uring mappings.
	return syscall.Mmap(fd, int64(offset), size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
}

func ptrUint32(b []byte, off uint32) *uint32 {
	return (*uint32)(unsafe.Pointer(&b[off]))
}

func (r *ioUring) unmapRings() {
	if len(r.sqesMap) > 0 {
		_ = syscall.Munmap(r.sqesMap)
		r.sqesMap = nil
	}
	if r.singleMap {
		if len(r.sqRing) > 0 {
			_ = syscall.Munmap(r.sqRing)
		}
	} else {
		if len(r.sqRing) > 0 {
			_ = syscall.Munmap(r.sqRing)
		}
		if len(r.cqRing) > 0 {
			_ = syscall.Munmap(r.cqRing)
		}
	}
	r.sqRing, r.cqRing = nil, nil
}

func (r *ioUring) abort() {
	if r == nil || r.fd < 0 {
		return
	}
	_ = syscall.Close(r.fd)
	r.fd = -1
}

func (r *ioUring) close() {
	if r == nil {
		return
	}
	r.abort()
	r.unmapRings()
}

func (r *ioUring) submitSQEs(count uint32, fill func(uint32, *ioUringSQE)) error {
	head := atomic.LoadUint32(r.sqHead)
	tail := atomic.LoadUint32(r.sqTail)
	entries := atomic.LoadUint32(r.sqEntries)
	if tail-head+count > entries {
		return syscall.EBUSY
	}
	mask := atomic.LoadUint32(r.sqMask)
	for i := uint32(0); i < count; i++ {
		idx := (tail + i) & mask
		sqe := &r.sqes[idx]
		*sqe = ioUringSQE{}
		fill(i, sqe)
		r.sqArray[idx] = idx
	}
	atomic.StoreUint32(r.sqTail, tail+count)

	remaining := count
	for remaining > 0 {
		submitted, _, errno := syscall.RawSyscall6(linuxSYSIOUringEnter, uintptr(r.fd), uintptr(remaining), 0, 0, 0, 0)
		if errno == syscall.EINTR {
			continue
		}
		if errno != 0 {
			if remaining == count {
				atomic.StoreUint32(r.sqTail, tail)
				return errno
			}
			// Some SQEs were already consumed. The remaining linked SQEs
			// still reference caller-owned memory, so keep retrying rather
			// than returning and allowing that memory to be reused.
			runtime.Gosched()
			continue
		}
		if submitted == 0 {
			runtime.Gosched()
			continue
		}
		if submitted >= uintptr(remaining) {
			return nil
		}
		remaining -= uint32(submitted)
	}
	return nil
}

func (r *ioUring) submitAccept(listenerFD int, multishot bool) error {
	return r.submitSQEs(1, func(_ uint32, sqe *ioUringSQE) {
		sqe.Opcode = ioUringOpAccept
		sqe.FD = int32(listenerFD)
		sqe.OpFlags = uint32(syscall.SOCK_NONBLOCK | syscall.SOCK_CLOEXEC)
		sqe.UserData = ioUringAcceptUserData
		if multishot {
			sqe.IOPrio = ioUringAcceptMultishot
		}
	})
}

func (r *ioUring) drainCQ(fn func(ioUringCQE)) int {
	head := atomic.LoadUint32(r.cqHead)
	tail := atomic.LoadUint32(r.cqTail)
	mask := atomic.LoadUint32(r.cqMask)
	n := 0
	for head != tail {
		cqe := r.cqes[head&mask]
		fn(cqe)
		head++
		n++
	}
	if n > 0 {
		atomic.StoreUint32(r.cqHead, head)
	}
	return n
}

func probeIOUring(entries uint32) (available bool, features uint32, err error) {
	ring, err := setupIOUring(entries)
	if err != nil {
		return false, 0, err
	}
	defer ring.close()
	features = ring.params.Features
	if err := ring.requireNetworkOps(); err != nil {
		return false, features, err
	}
	return true, features, nil
}

func (r *ioUring) requireNetworkOps() error {
	const maxOps = 256
	buffer := make([]byte, int(unsafe.Sizeof(ioUringProbe{}))+maxOps*int(unsafe.Sizeof(ioUringProbeOp{})))
	header := (*ioUringProbe)(unsafe.Pointer(&buffer[0]))
	_, _, errno := syscall.RawSyscall6(
		linuxSYSIOUringRegister, uintptr(r.fd), uintptr(ioUringRegisterProbe),
		uintptr(unsafe.Pointer(header)), uintptr(maxOps), 0, 0,
	)
	if errno != 0 {
		return fmt.Errorf("fh: io_uring operation probe: %w", errno)
	}
	length := int(header.OpsLen)
	if length > maxOps {
		length = maxOps
	}
	base := unsafe.Add(unsafe.Pointer(header), unsafe.Sizeof(ioUringProbe{}))
	ops := unsafe.Slice((*ioUringProbeOp)(base), length)
	required := map[uint8]string{
		ioUringOpAccept:      "accept",
		ioUringOpRecv:        "recv",
		ioUringOpSend:        "send",
		ioUringOpLinkTimeout: "link_timeout",
	}
	for _, op := range ops {
		if op.Flags&ioUringOpSupported != 0 {
			delete(required, op.Op)
		}
	}
	if len(required) != 0 {
		missing := make([]string, 0, len(required))
		for _, name := range required {
			missing = append(missing, name)
		}
		slices.Sort(missing)
		return fmt.Errorf("fh: io_uring missing required network operations: %s", strings.Join(missing, ", "))
	}
	return nil
}

type ioUringShard struct {
	id            int
	listener      *kernelFDListener
	ring          *ioUring
	waitEPFD      int
	wakeR         int
	wakeW         int
	cfg           KernelConfig
	host          Host
	tlsWrap       func(net.Conn) net.Conn
	closed        *atomic.Bool
	multishot     bool
	closeOnce     sync.Once
	pressureDelay time.Duration

	submitMu     sync.Mutex
	requests     sync.Map
	requestCount atomic.Int64
	nextID       atomic.Uint64
	conns        sync.Map
	connCount    atomic.Int64
}

func newIOUringShard(id int, listener *kernelFDListener, cfg KernelConfig, host Host, tlsWrap func(net.Conn) net.Conn, closed *atomic.Bool) (*ioUringShard, error) {
	ring, err := setupIOUring(cfg.IOUringEntries)
	if err != nil {
		return nil, err
	}
	waitFD, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		ring.close()
		return nil, err
	}
	var pipe [2]int
	if err = syscall.Pipe2(pipe[:], syscall.O_NONBLOCK|syscall.O_CLOEXEC); err != nil {
		_ = syscall.Close(waitFD)
		ring.close()
		return nil, err
	}
	fail := func(cause error) (*ioUringShard, error) {
		_ = syscall.Close(pipe[0])
		_ = syscall.Close(pipe[1])
		_ = syscall.Close(waitFD)
		ring.close()
		return nil, cause
	}
	if err = syscall.EpollCtl(waitFD, syscall.EPOLL_CTL_ADD, ring.fd, &syscall.EpollEvent{Events: uint32(syscall.EPOLLIN), Fd: int32(ring.fd)}); err != nil {
		return fail(err)
	}
	if err = syscall.EpollCtl(waitFD, syscall.EPOLL_CTL_ADD, pipe[0], &syscall.EpollEvent{Events: uint32(syscall.EPOLLIN), Fd: int32(pipe[0])}); err != nil {
		return fail(err)
	}
	shard := &ioUringShard{id: id, listener: listener, ring: ring, waitEPFD: waitFD, wakeR: pipe[0], wakeW: pipe[1], cfg: cfg, host: host, tlsWrap: tlsWrap, closed: closed, multishot: true}
	shard.nextID.Store(ioUringAcceptUserData)
	return shard, nil
}
func (s *ioUringShard) wake() {
	if s != nil && s.wakeW >= 0 {
		_, _ = syscall.Write(s.wakeW, []byte{1})
	}
}
func (s *ioUringShard) close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		if s.wakeR >= 0 {
			_ = syscall.Close(s.wakeR)
			s.wakeR = -1
		}
		if s.wakeW >= 0 {
			_ = syscall.Close(s.wakeW)
			s.wakeW = -1
		}
		if s.waitEPFD >= 0 {
			_ = syscall.Close(s.waitEPFD)
			s.waitEPFD = -1
		}
		s.ring.close()
	})
}
func (s *ioUringShard) run() (runErr error) {
	defer func() {
		if runErr != nil {
			s.failAllIO(runErr)
		}
	}()
	if s.cfg.PinThreads {
		cpu, e := reactorCPU(s.id, s.cfg)
		if e == nil {
			var restore func()
			restore, e = pinCurrentThread(cpu)
			if e == nil {
				s.host.pinned(1)
				defer func() { s.host.pinned(-1); restore() }()
			}
		}
		if e != nil {
			if s.cfg.Required {
				return e
			}
			s.host.warn("fh: io_uring reactor CPU affinity unavailable", "reactor", s.id, "error", e)
		}
	}
	if e := s.submitAccept(true); e != nil {
		return e
	}
	acceptActive := true
	var events [16]syscall.EpollEvent
	var wakeBuf [64]byte
	for {
		closing := s.closed.Load() || s.host.closed()
		if closing && s.connCount.Load() == 0 && s.requestCount.Load() == 0 {
			return nil
		}
		n, e := syscall.EpollWait(s.waitEPFD, events[:], -1)
		if e != nil {
			if errors.Is(e, syscall.EINTR) {
				continue
			}
			if closing && errors.Is(e, syscall.EBADF) {
				return nil
			}
			return e
		}
		wakeSeen, ringSeen := false, false
		for i := 0; i < n; i++ {
			switch int(events[i].Fd) {
			case s.wakeR:
				wakeSeen = true
				for {
					if _, x := syscall.Read(s.wakeR, wakeBuf[:]); x != nil {
						break
					}
				}
			case s.ring.fd:
				ringSeen = true
			}
		}
		if wakeSeen {
			closing = s.closed.Load() || s.host.closed()
			if closing && s.connCount.Load() == 0 && s.requestCount.Load() == 0 {
				return nil
			}
		}
		if !ringSeen {
			continue
		}
		var completionErr error
		var pressurePause time.Duration
		s.ring.drainCQ(func(cqe ioUringCQE) {
			if cqe.UserData != ioUringAcceptUserData {
				s.completeIO(cqe)
				return
			}
			more := cqe.Flags&ioUringCQEMore != 0
			if cqe.Res >= 0 {
				s.pressureDelay = 0
				s.handleAcceptedFD(int(cqe.Res))
			} else {
				errno := syscall.Errno(-cqe.Res)
				switch errno {
				case syscall.EINTR, syscall.ECONNABORTED, syscall.EPROTO, syscall.EAGAIN:
				case syscall.EINVAL:
					if s.multishot {
						s.multishot = false
					} else {
						completionErr = errors.New("fh: io_uring accept operation is unsupported")
					}
				case syscall.ECANCELED, syscall.EBADF:
					if closing {
						acceptActive = false
						return
					}
				case syscall.EMFILE, syscall.ENFILE, syscall.ENOBUFS, syscall.ENOMEM:
					s.host.acceptError()
					s.pressureDelay = nextAcceptBackoff(s.pressureDelay, s.cfg)
					pressurePause = s.pressureDelay
				default:
					s.host.acceptError()
				}
			}
			if !more {
				acceptActive = false
			}
		})
		if completionErr != nil {
			return completionErr
		}
		if pressurePause > 0 {
			time.Sleep(pressurePause)
		}
		closing = s.closed.Load() || s.host.closed()
		if !acceptActive && !closing {
			if e := s.submitAccept(s.multishot); e != nil {
				return e
			}
			acceptActive = true
		}
	}
}
func (s *ioUringShard) submitAccept(multishot bool) error {
	s.submitMu.Lock()
	defer s.submitMu.Unlock()
	return s.ring.submitAccept(s.listener.fd, multishot)
}
func (s *ioUringShard) handleAcceptedFD(fd int) {
	count, e := configureAcceptedFD(fd, s.cfg)
	if count > 0 {
		s.host.socketOptionErrors(count)
	}
	if e != nil {
		_ = syscall.Close(fd)
		s.host.acceptError()
		return
	}
	c, e := newIOUringConn(fd, s)
	if e != nil {
		s.host.acceptError()
		return
	}
	var accepted net.Conn = c
	if s.tlsWrap != nil {
		accepted = s.tlsWrap(accepted)
	}
	s.host.accept(accepted)
}
