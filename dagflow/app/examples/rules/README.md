# Rule example

`notification_approval_demo` uses named conditions from `app/bcl/20-workflows/conditions.bcl`.

Expected outcomes:

- `user@example.com` -> completes.
- `blocked@blocked.test` -> rejected at `validate`, no retry.
- subject `approval` or `sensitive` -> waits for approval at `send`.
