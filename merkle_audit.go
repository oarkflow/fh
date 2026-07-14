package fh

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// MerkleCheckpoint represents a batch of audit events combined into a Merkle root.
type MerkleCheckpoint struct {
	// Sequence is the checkpoint sequence number.
	Sequence uint64 `json:"sequence"`

	// RootHash is the Merkle root of all events in this checkpoint.
	RootHash string `json:"root_hash"`

	// EventCount is the number of events in this checkpoint.
	EventCount int `json:"event_count"`

	// PrevCheckpointHash is the hash of the previous checkpoint for chaining.
	PrevCheckpointHash string `json:"prev_checkpoint_hash"`

	// StartTime is the time the first event in this checkpoint was received.
	StartTime time.Time `json:"start_time"`

	// EndTime is the time the last event in this checkpoint was received.
	EndTime time.Time `json:"end_time"`

	// ServerID identifies the server that created this checkpoint.
	ServerID string `json:"server_id"`

	// Signature is an optional HMAC signature of the checkpoint.
	Signature string `json:"signature,omitempty"`
}

// MerkleAuditSink wraps an AuditSink and periodically creates Merkle checkpoints
// from batches of audit events. Each checkpoint combines the events into a Merkle
// tree and publishes the root hash, making deletion or modification of historical
// audit events detectable.
type MerkleAuditSink struct {
	mu            sync.Mutex
	next          AuditSink
	bucket        []auditLeaf
	bucketMax     int
	checkpointFn  func(MerkleCheckpoint)
	prevRoot      string
	sequence      uint64
	serverID      string
	stopCh        chan struct{}
	once          sync.Once
}

type auditLeaf struct {
	eventHash string
	data      []byte
}

// MerkleConfig configures the Merkle audit checkpoint system.
type MerkleConfig struct {
	// BucketSize is the number of events per checkpoint. Default: 100.
	BucketSize int

	// CheckpointInterval is the maximum time between checkpoints. Default: 5 minutes.
	CheckpointInterval time.Duration

	// ServerID identifies this server in checkpoints.
	ServerID string

	// OnCheckpoint is called when a new checkpoint is created.
	OnCheckpoint func(MerkleCheckpoint)

	// Sink is the underlying audit sink to write events to.
	Sink AuditSink
}

// NewMerkleAuditSink creates a Merkle checkpoint audit sink.
func NewMerkleAuditSink(cfg MerkleConfig) *MerkleAuditSink {
	if cfg.BucketSize <= 0 {
		cfg.BucketSize = 100
	}
	if cfg.CheckpointInterval <= 0 {
		cfg.CheckpointInterval = 5 * time.Minute
	}

	s := &MerkleAuditSink{
		next:        cfg.Sink,
		bucket:      make([]auditLeaf, 0, cfg.BucketSize),
		bucketMax:   cfg.BucketSize,
		checkpointFn: cfg.OnCheckpoint,
		serverID:    cfg.ServerID,
		stopCh:      make(chan struct{}),
	}

	go s.flushLoop(cfg.CheckpointInterval)
	return s
}

// WriteAudit records an event and adds it to the current Merkle bucket.
func (s *MerkleAuditSink) WriteAudit(ctx context.Context, e AuditEvent) error {
	if s == nil || s.next == nil {
		return nil
	}

	b, err := json.Marshal(e)
	if err != nil {
		return err
	}

	leafHash := sha256.Sum256(b)
	hash := hex.EncodeToString(leafHash[:])

	s.mu.Lock()
	s.bucket = append(s.bucket, auditLeaf{eventHash: hash, data: b})
	shouldCheckpoint := len(s.bucket) >= s.bucketMax
	s.mu.Unlock()

	if shouldCheckpoint {
		s.createCheckpoint()
	}

	return s.next.WriteAudit(ctx, e)
}

// createCheckpoint combines the current bucket into a Merkle root and publishes it.
func (s *MerkleAuditSink) createCheckpoint() {
	s.mu.Lock()
	if len(s.bucket) == 0 {
		s.mu.Unlock()
		return
	}

	leaves := s.bucket
	s.bucket = make([]auditLeaf, 0, s.bucketMax)
	s.sequence++
	seq := s.sequence
	prevRoot := s.prevRoot
	now := time.Now()
	s.mu.Unlock()

	// Build Merkle tree from leaf hashes.
	hashes := make([]string, len(leaves))
	for i, leaf := range leaves {
		hashes[i] = leaf.eventHash
	}

	root := buildMerkleRoot(hashes)

	checkpoint := MerkleCheckpoint{
		Sequence:           seq,
		RootHash:           root,
		EventCount:         len(leaves),
		PrevCheckpointHash: prevRoot,
		StartTime:          now,
		EndTime:            now,
		ServerID:           s.serverID,
	}

	s.mu.Lock()
	s.prevRoot = root
	s.mu.Unlock()

	if s.checkpointFn != nil {
		s.checkpointFn(checkpoint)
	}
}

// VerifyCheckpoint verifies that a checkpoint's Merkle root matches the given event hashes.
func VerifyCheckpoint(cp MerkleCheckpoint, eventHashes []string) bool {
	if len(eventHashes) == 0 {
		return cp.RootHash == emptyRootHash()
	}
	root := buildMerkleRoot(eventHashes)
	return root == cp.RootHash
}

// VerifyChain verifies that a chain of checkpoints is valid by checking
// PrevCheckpointHash links and event count continuity.
func VerifyChain(checkpoints []MerkleCheckpoint) error {
	if len(checkpoints) == 0 {
		return nil
	}

	for i := 1; i < len(checkpoints); i++ {
		if checkpoints[i].PrevCheckpointHash != checkpoints[i-1].RootHash {
			return fmt.Errorf("checkpoint %d: prev hash mismatch", checkpoints[i].Sequence)
		}
		if checkpoints[i].Sequence != checkpoints[i-1].Sequence+1 {
			return fmt.Errorf("checkpoint %d: sequence gap (expected %d)", checkpoints[i].Sequence, checkpoints[i-1].Sequence+1)
		}
	}
	return nil
}

// flushLoop periodically flushes the bucket even if it hasn't reached bucketMax.
func (s *MerkleAuditSink) flushLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.createCheckpoint()
		case <-s.stopCh:
			return
		}
	}
}

// Close stops the flush loop and creates a final checkpoint if needed.
func (s *MerkleAuditSink) Close() error {
	s.once.Do(func() { close(s.stopCh) })
	s.createCheckpoint()
	if c, ok := s.next.(AuditSinkCloser); ok {
		return c.Close()
	}
	return nil
}

// ── Merkle tree helpers ─────────────────────────────────────────────────────

func buildMerkleRoot(hashes []string) string {
	if len(hashes) == 0 {
		return emptyRootHash()
	}

	nodes := make([]string, len(hashes))
	copy(nodes, hashes)

	for len(nodes) > 1 {
		var next []string
		for i := 0; i < len(nodes); i += 2 {
			if i+1 < len(nodes) {
				combined := sha256.Sum256([]byte(nodes[i] + nodes[i+1]))
				next = append(next, hex.EncodeToString(combined[:]))
			} else {
				next = append(next, nodes[i])
			}
		}
		nodes = next
	}

	return nodes[0]
}

func emptyRootHash() string {
	h := sha256.Sum256([]byte{})
	return hex.EncodeToString(h[:])
}

// MerkleAuditMiddleware creates middleware that records audit events with
// Merkle checkpoint support.
func MerkleAuditMiddleware(sink *MerkleAuditSink) HandlerFunc {
	return func(c Ctx) error {
		err := c.Next()
		if err == nil {
			e := AuditEvent{
				Action:   "request.completed",
				Result:   "success",
				Method:   c.Method(),
				Path:     c.Path(),
				RequestID: c.Get("X-Request-ID"),
			}
			_ = sink.WriteAudit(c.Context(), e)
		}
		return err
	}
}
