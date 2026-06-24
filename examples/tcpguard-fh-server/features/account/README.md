# Account login protection

## Endpoint

`POST /api/v1/account/login`

## Purpose

Demonstrates behavioral account takeover checks using headers such as new device, previous country, current country, account status, session drift, and external risk data.

## Curl

```bash
curl -i -X POST \
  -H 'X-User-ID: user-1' \
  -H 'X-New-Device: true' \
  -H 'X-Previous-Country: US' \
  -H 'X-Country: NP' \
  http://127.0.0.1:18184/api/v1/account/login
```

## Expected response

Expected status is a TCPGuard challenge-style response. Headers include `X-TCPGuard-Decision: challenge`, `X-TCPGuard-Severity`, `X-TCPGuard-Trace`, and `X-TCPGuard-Message`.
