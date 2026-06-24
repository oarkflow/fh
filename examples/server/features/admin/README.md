# Admin endpoint protection

## Endpoint

`POST /admin/users`

## Purpose

Demonstrates high-risk administrative action handling. TCPGuard can challenge or block after-hours admin changes and can combine role, tenant, path, method, and business context.

## Curl

```bash
curl -i -X POST \
  -H 'X-User-ID: manager-1' \
  -H 'X-User-Role: admin' \
  -H 'X-Outside-Hours: true' \
  http://127.0.0.1:18184/admin/users
```

## Expected response

Expected status is a challenge or forbidden response depending on rule effect. A clean allowed response is:

```json
{"ok":true,"message":"admin change accepted"}
```
