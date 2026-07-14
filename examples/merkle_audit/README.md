# Merkle Audit Example

Demonstrates tamper-evident audit logging with Merkle checkpoints.

## What it does

- Batches audit events into Merkle trees (50 events per checkpoint)
- Checkpoints chain via PrevCheckpointHash
- Writes to .fh-reliability/audit.jsonl
- Provides checkpoint chain verification endpoint

## Run

```bash
go run examples/merkle_audit/main.go
```

## Test

```bash
# Create audit-logged transfer
curl -X POST http://localhost:3000/transfer

# Verify checkpoint chain
curl http://localhost:3000/admin/audit/checkpoint

# Inspect audit log
cat .fh-reliability/audit.jsonl
```
