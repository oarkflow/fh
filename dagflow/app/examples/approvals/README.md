# Approval example

Trigger approval:

```bash
curl -X POST 'http://localhost:8080/ops/queues/email_jobs/workflows/notification_approval_demo/enqueue?await=false' \
  -H 'X-API-Key: dev-secret' \
  -H 'Content-Type: application/json' \
  -d '{"to":"user@example.com","subject":"approval","body":"needs approval"}'
```

List and approve:

```bash
curl http://localhost:8080/ops/approvals?status=pending -H 'X-API-Key: dev-secret'
curl -X POST http://localhost:8080/ops/tasks/<task_id>/approve -H 'X-API-Key: dev-secret' -H 'Content-Type: application/json' -d '{"approver":"ops","reason":"approved"}'
```
