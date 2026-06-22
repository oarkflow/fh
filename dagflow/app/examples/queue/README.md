# Queue example

Run the server and enqueue a workflow task:

```bash
curl -X POST 'http://localhost:8080/ops/queues/email_jobs/workflows/notification_approval_demo/enqueue?await=true' \
  -H 'X-API-Key: dev-secret' \
  -H 'Content-Type: application/json' \
  -d '{"to":"user@example.com","subject":"hello","body":"queued message"}'
```

Pause, resume, and purge queue:

```bash
curl -X POST http://localhost:8080/ops/queues/email_jobs/pause -H 'X-API-Key: dev-secret'
curl -X POST http://localhost:8080/ops/queues/email_jobs/resume -H 'X-API-Key: dev-secret'
curl -X POST 'http://localhost:8080/ops/queues/email_jobs/purge?confirm=true' -H 'X-API-Key: dev-secret'
```
