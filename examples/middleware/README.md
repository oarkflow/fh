# Middleware example

Run from the repository root:

```sh
go run ./examples/middleware
```

Static, dynamic, and named-wildcard rewrites:

```sh
curl -i http://localhost:3000/legacy
curl -i http://localhost:3000/old-users/42
curl -i http://localhost:3000/old-docs/api/v2/auth
```

The server ceiling is 2 MiB, the global middleware ceiling is 1 MiB, and
`POST /small` has an endpoint-specific 8 KiB ceiling:

```sh
head -c 9000 /dev/zero | curl -i --data-binary @- http://localhost:3000/small
```

Observe `X-Cache: MISS`, then `X-Cache: HIT`:

```sh
curl -i http://localhost:3000/cached
curl -i http://localhost:3000/cached
```

Get a CSRF cookie/token, then submit both values:

```sh
curl -i -c cookies.txt http://localhost:3000/csrf-token
curl -i -b cookies.txt -H 'X-CSRF-Token: TOKEN_FROM_JSON' -X POST http://localhost:3000/small
```

Unsafe early-data requests receive `425 Too Early` unless an idempotency key
is present:

```sh
curl -i -X POST -H 'Early-Data: 1' http://localhost:3000/small
curl -i -X POST -H 'Early-Data: 1' -H 'Idempotency-Key: demo-1' http://localhost:3000/small
```

Accepted CORS preflight:

```sh
curl -i -X OPTIONS http://localhost:3000/small \
  -H 'Origin: http://localhost:5173' \
  -H 'Access-Control-Request-Method: POST' \
  -H 'Access-Control-Request-Headers: X-CSRF-Token'
```
