# Dynamic owner and path protection

## Endpoint

`PUT /api/users/user-2/order/order-9`

## Purpose

Demonstrates path-aware authorization and business ownership rules. TCPGuard can compare request identity against dynamic path/entity context.

## Curl

```bash
curl -i -X PUT \
  -H 'X-User-ID: user-1' \
  http://127.0.0.1:18184/api/users/user-2/order/order-9
```

## Expected response

A mismatched user/order owner should receive a challenge or deny response. A matching owner can proceed to:

```json
{"ok":true,"message":"user/order update accepted"}
```
