# merkle_audit

Merkle-tree based audit checkpoints for fh. Periodically combines audit records into tamper-evident Merkle roots.

## Why

Individual audit log entries can be tampered with by an attacker with filesystem access. Merkle checkpoints combine batches of events into a tree structure where any modification to any event changes the root hash. Chaining checkpoints via PrevCheckpointHash creates a tamper-evident chain that detects deletion or modification of historical records.

## Features

- Batch audit events into Merkle trees
- Configurable bucket size and flush interval
- Chain verification across checkpoints
- HMAC signature support on checkpoints
- Integration with existing hash chain audit sink
- Server identification in checkpoints

## Usage

```go
app := fh.New()

fileSink, _ := fh.OpenFileAuditSink(".fh-reliability/audit.jsonl")
defer fileSink.Close()

merkleSink := fh.NewMerkleAuditSink(fh.MerkleConfig{
    BucketSize:         50,
    CheckpointInterval: 1 * time.Minute,
    ServerID:           "api-server-1",
    OnCheckpoint: func(cp fh.MerkleCheckpoint) {
        log.Printf("Checkpoint #%d: %d events, root=%s",
            cp.Sequence, cp.EventCount, cp.RootHash[:16])
    },
    Sink: fileSink,
})
defer merkleSink.Close()

// Apply middleware
app.Use(fh.MerkleAuditMiddleware(merkleSink))

app.Post("/transfer", func(c fh.Ctx) error {
    c.Audit().Record("transfer.created", "account", "acct-123",
        map[string]any{"amount": 100.00})
    return c.JSON(fh.Map{"status": "ok"})
})

// Verify checkpoint chain
app.Get("/admin/audit/verify", func(c fh.Ctx) error {
    checkpoints := []fh.MerkleCheckpoint{cp1, cp2, cp3}
    err := fh.VerifyChain(checkpoints)
    return c.JSON(fh.Map{"valid": err == nil})
})
```

## MerkleCheckpoint

```go
type MerkleCheckpoint struct {
    Sequence           uint64
    RootHash           string
    EventCount         int
    PrevCheckpointHash string
    StartTime          time.Time
    EndTime            time.Time
    ServerID           string
}
```

## Verification

- `VerifyCheckpoint(cp, hashes)` - verify a single checkpoint's root matches its events
- `VerifyChain(checkpoints)` - verify PrevCheckpointHash links and sequence continuity
