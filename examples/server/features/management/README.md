# TCPGuard management API

## Server

`http://127.0.0.1:18185`

## Purpose

Demonstrates the upstream TCPGuard management server: health, hot reload, simulate, explain, incidents, audit, audit verification, approvals, approve, and reject.

## Authentication

Default API key: `dev-management-key`

Override with:

```bash
TCPGUARD_MGMT_API_KEY='your-key' go run .
```

## Curl

```bash
curl -i -H 'Authorization: Bearer dev-management-key' http://127.0.0.1:18185/health
curl -i -X POST -H 'Authorization: Bearer dev-management-key' http://127.0.0.1:18185/reload
```

## Expected response

JSON responses from the upstream TCPGuard management server. Access is restricted to localhost CIDRs and configured roles.
