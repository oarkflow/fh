package main

import (
	"fmt"
	"time"

	"github.com/oarkflow/fh"
)

func main() {
	app := fh.New()

	// Create Merkle audit sink with periodic checkpoints
	fileSink, err := fh.OpenFileAuditSink(".fh-reliability/audit.jsonl")
	if err != nil {
		panic(err)
	}
	defer fileSink.Close()

	merkleSink := fh.NewMerkleAuditSink(fh.MerkleConfig{
		BucketSize:         50,
		CheckpointInterval: 1 * time.Minute,
		ServerID:           "api-server-1",
		OnCheckpoint: func(cp fh.MerkleCheckpoint) {
			fmt.Printf("Merkle checkpoint #%d: %d events, root=%s\n",
				cp.Sequence, cp.EventCount, cp.RootHash[:16])
		},
		Sink: fileSink,
	})
	defer merkleSink.Close()

	// Wrap with hash chain for extra tamper evidence
	_ = fh.NewHashChainAuditSink(merkleSink)

	// All requests are audit-logged with Merkle checkpoints
	app.Use(fh.MerkleAuditMiddleware(merkleSink))

	app.Post("/transfer", func(c fh.Ctx) error {
		// Record business audit event
		c.Audit().Record("transfer.created", "account", "acct-123",
			map[string]any{
				"amount":   100.00,
				"currency": "USD",
			})

		return c.JSON(fh.Map{"status": "transferred"})
	})

	app.Get("/admin/audit/checkpoint", func(c fh.Ctx) error {
		// Verify a checkpoint chain
		cp1 := fh.MerkleCheckpoint{Sequence: 1, RootHash: "abc123"}
		cp2 := fh.MerkleCheckpoint{Sequence: 2, RootHash: "def456", PrevCheckpointHash: "abc123"}
		err := fh.VerifyChain([]fh.MerkleCheckpoint{cp1, cp2})
		return c.JSON(fh.Map{
			"chain_valid": err == nil,
		})
	})

	fmt.Println("Merkle audit example on :3000")
	fmt.Println("  POST /transfer              - audit-logged transfer")
	fmt.Println("  GET  /admin/audit/checkpoint - verify checkpoint chain")
	fmt.Println("  Checkpoints written to .fh-reliability/audit.jsonl")
	app.Listen(":3000")
}
