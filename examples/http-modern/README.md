# Modern HTTP features

This example demonstrates the dependency-free modern HTTP features in `fh`:

- `103 Early Hints` with custom `Link` attributes
- General informational responses with `SendInformational`
- `PushResource` helpers for stylesheets, scripts, and fonts
- RFC 9218 `Priority` request parsing and response generation
- HTTP/2 support for Early Hints and resource push, with HTTP/1.1 Early Hints fallback

Run it from the repository root:

```bash
go run ./examples/http-modern
```

Then inspect the responses:

```bash
curl -i http://localhost:8084/early-hints
curl -i http://localhost:8084/resources
curl -i -H 'Priority: u=1, i' http://localhost:8084/priority
```

The priority endpoint returns the parsed request priority and emits a `Priority`
response header. Use an HTTP/2-capable client against a TLS listener to inspect
HTTP/2 informational responses and push behavior; the plain listener is useful
for observing the HTTP/1.1 `103 Early Hints` fallback.
