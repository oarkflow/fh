# RFC 9421 signed-response example

This example uses `mw/httpsignature` and `pkg/httpsignature` to negotiate and verify nonce-bound Ed25519 response signatures.

## Run

Generate a persistent key pair:

```sh
go run ./examples/rfc9421/server -generate-key
```

Export the printed values, then start the server:

```sh
export FH_RFC9421_PRIVATE_KEY='<printed private seed>'
export FH_RFC9421_PUBLIC_KEY='<printed public key>'
go run ./examples/rfc9421/server
```

Run the fail-closed Go client:

```sh
go run ./examples/rfc9421/client \
  -public-key "$FH_RFC9421_PUBLIC_KEY" \
  -url http://127.0.0.1:8081/api/message
```

Or open `http://127.0.0.1:8081` for the WebCrypto client. The browser example fetches the public key from the demonstration server only to remain runnable. That is not an independent trust anchor: production browser/native clients must embed the public key or obtain it through an independently authenticated release/configuration channel.

The browser example is same-origin. For an explicitly allowed cross-origin deployment, CORS must expose `Content-Digest`, `Signature-Input`, and `Signature`; never use a wildcard origin with credentials.

## Wire profile

The client sends:

```http
Accept-Signature: sig1=("@status" "content-digest" "content-type" "@method";req "@target-uri";req);created;expires;nonce="<unique-base64url>";keyid="response-signing-2026-01";alg="ed25519";tag="fh-rfc9421-response"
```

The response includes RFC 9530 `Content-Digest` and RFC 9421 `Signature-Input`/`Signature` fields. Verification rejects missing or modified bodies, content types, statuses, request method/URI bindings, nonces, key IDs, timestamps, algorithms, tags, or signatures.

The profile intentionally rejects content-coded and streaming responses. HTTPS is still required for confidentiality and to protect metadata that is outside the covered components.
