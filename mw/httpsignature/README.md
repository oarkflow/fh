# RFC 9421 HTTP response signatures

`mw/httpsignature` implements a strict, interoperable RFC 9421 response-signature profile using Ed25519 and an RFC 9530 `Content-Digest`.

The client sends `Accept-Signature` with a unique nonce. The server signs:

- `@status`
- `content-digest`
- `content-type`
- the originating request's `@method` using `;req`
- the originating request's `@target-uri` using `;req`

The signature parameters include `created`, `expires`, `nonce`, `keyid`, `alg="ed25519"`, and `tag="fh-rfc9421-response"`.

```go
middleware, err := httpsignature.New(httpsignature.Config{
    PrivateKey: privateKey,
    KeyID:      "response-signing-2026-01",
    Origin:     "https://api.example.com",
})
if err != nil {
    log.Fatal(err)
}
app.Use(middleware)
```

`AllowedOrigins` can list additional exact origins when one server intentionally supports multiple authorities. The middleware selects the signing origin from the validated request `Host`, so `@target-uri` continues to match what the client requested. The secure WASM example uses this only for the development authorities `localhost`, `127.0.0.1`, and `0.0.0.0`; production should normally configure one HTTPS origin.

Use `pkg/httpsignature.Client` for a fail-closed Go client. See `examples/rfc9421` for Go and browser implementations.

The negotiated profile deliberately rejects `Content-Encoding`; otherwise an intermediary could modify an unsigned content-coding header and change how authenticated bytes are interpreted. Install it after authorization and as the last response-transform middleware. Do not enable compression on these routes or use streaming responses because the final content must be buffered and digested. Use HTTPS even though responses are signed.

The default replay store is bounded and process-local. Multi-instance deployments must provide a distributed atomic `NonceStore`.
