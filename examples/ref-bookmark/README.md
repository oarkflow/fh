# REF bookmark service

This example implements a bookmark API in BCL, hosted by a thin Go HTTP server. It demonstrates PostgreSQL-backed accounts and bookmarks, cookie/JWT authentication, owner-scoped CRUD, request path/query binding, memory cache-aside reads, rate limits, route audit, an in-memory analytics outbox, file-backed jobs, and a scheduled health-check process.

This documents the checked-in behavior and its operational limits.

## Run

Requires Go and PostgreSQL. Create a database and set:

~~~sh
createdb bookmarks
export DATABASE_URL='postgres://postgres:postgres@localhost/bookmarks?sslmode=disable'
export JWT_SECRET="$(openssl rand -hex 32)"
go run ./examples/ref-bookmark -addr :8090
~~~

Migrations create the users, bookmarks, and analytics tables at startup. The server fails fast if required configuration or PostgreSQL is unavailable. Queue files are written under .data/bookmarks/jobs relative to the working directory.

## API walkthrough

Registration returns the inserted user row. Login sets the bookmark_sid session cookie and returns a JWT in token; subsequent requests may use the cookie or Authorization: Bearer <token>.

~~~sh
BASE=http://localhost:8090
curl -i -X POST "$BASE/auth/register" -H 'Content-Type: application/json' -d '{"email":"ada@example.com","name":"Ada","password":"correct horse battery staple"}'
curl -i -c /tmp/bookmarks.cookies -X POST "$BASE/auth/login" -H 'Content-Type: application/json' -d '{"email":"ada@example.com","password":"correct horse battery staple"}'
~~~

Use the returned token for authenticated requests:

~~~sh
TOKEN='<token from login response>'
curl -i "$BASE/bookmarks" -H "Authorization: Bearer $TOKEN"
curl -i -X POST "$BASE/bookmarks" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"url":"https://go.dev","title":"Go","description":"Go language","tags":["golang"]}'
~~~

The create response contains the generated id. Use it for path-based operations:

~~~sh
ID='<bookmark id>'
curl -i "$BASE/bookmarks/$ID" -H "Authorization: Bearer $TOKEN"
curl -i "$BASE/bookmarks/search?query=go" -H "Authorization: Bearer $TOKEN"
curl -i "$BASE/go/$ID"
curl -i -X PUT "$BASE/bookmarks/$ID" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"title":"The Go language"}'
curl -i -X DELETE "$BASE/bookmarks/$ID" -H "Authorization: Bearer $TOKEN"
curl -i "$BASE/health"
~~~

Routes carrying :id use the request.param action with from path; search reads query from the URL with from query. JSON body fields remain the intent input.

## Implemented features

- app.bcl declares resources, schemas, roles, intents, routes, a worker, schedule, and durable process.
- Authentication accepts the signed session cookie or JWT bearer token. Bookmark reads and mutations are scoped to the caller except public /go/:id resolution.
- List uses memory cache-aside. Create, update, and delete are represented in BCL; update/delete invalidate the caller's list cache.
- Create has a route-level rate limit and audit policy. Audit omits request content and redacts url.
- Analytics events use outbox.memory. Jobs use queue.file; process run state uses the SQL store.
- The hourly process checks links and records health outcomes using the configured HTTP client.

## Operational limits

This is an instructional single-process example, not a production deployment template. cache.memory, session.memory, outbox.memory, and the in-process rate-limit store do not coordinate across replicas or survive process restarts. The file queue needs a persistent writable working directory. URL fetching currently permits all hosts and private network addresses for local demonstration; restrict these before exposing it to untrusted callers. The analytics outbox is in memory, so it is not a durable transactional outbox.

Bookmark creation does not fetch URL metadata. The circuit-breaker resource is used by the scheduled health-check process.

## Files

- app.bcl — application behavior and HTTP projection.
- main.go — config loading, route mounting, and server lifecycle.
- go.mod — standalone module wiring.
