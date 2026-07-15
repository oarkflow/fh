# RFC 9421 response signatures

FH provides a strict response-signature profile in `mw/httpsignature` and a verifier/client in `pkg/httpsignature`. It follows [RFC 9421](https://www.rfc-editor.org/rfc/rfc9421.html) and uses [RFC 9530](https://www.rfc-editor.org/rfc/rfc9530.html) `Content-Digest` values.

## Security property

The client creates a cryptographically random nonce and requests a response signature with `Accept-Signature`. The server signs the response status, content digest, content type, and the originating request method and absolute target URI. It also signs short `created` and `expires` timestamps, the nonce, key ID, Ed25519 algorithm, and profile tag.

The client releases the body only after all digest, request binding, lifetime, nonce, key ID, profile, and Ed25519 checks pass. Modification or replay therefore fails closed. HTTPS remains mandatory: signatures provide integrity and server-key authentication, not confidentiality, traffic-analysis protection, or integrity for metadata outside the profile.

## Server middleware

```go
signer, err := httpsignature.New(httpsignature.Config{
    PrivateKey: privateKey,
    KeyID:      "response-signing-2026-01",
    Origin:     "https://api.example.com",
    Validity:   90 * time.Second,
})
if err != nil {
    log.Fatal(err)
}
app.Use(signer)
```

`Origin` is explicit so `@target-uri` is never derived from untrusted forwarding headers. The middleware requires a matching negotiation, rejects reused nonces, caps buffered response size, sets `Cache-Control: no-store` and `Vary: Accept-Signature`, and signs the final transformed bytes.

Install it after authentication/authorization and as the last response-transform middleware. The profile intentionally rejects streaming and `Content-Encoding`; disable compression on signed routes. For multiple server instances, provide a distributed `NonceStore` whose `CheckAndStore` operation is atomic.

## Go client

```go
client := httpsignature.Client{
    HTTPClient: &http.Client{Timeout: 5 * time.Second},
    Verifier: httpsignature.Verifier{
        KeyID:       "response-signing-2026-01",
        PublicKey:   trustedPublicKey,
        ClockSkew:   30 * time.Second,
        MaxValidity: 2 * time.Minute,
    },
    MaxBodySize: 1 << 20,
}

response, err := client.Do(request)
if err != nil {
    // Do not consume or display an unverified response.
    return err
}
```

The public key must be embedded in the client or supplied through a separately authenticated update/configuration channel. Fetching the key beside the signed response does not defend against an attacker who can alter both.

See [`examples/rfc9421`](../examples/rfc9421) for a runnable server, Go client, and browser WebCrypto verifier. Cross-origin browser clients also require an allowlisted CORS policy that exposes `Content-Digest`, `Signature-Input`, and `Signature`.

## Combining with secure transport

For the strongest browser transport profile, register `mw/securetransport` first and `mw/httpsignature` immediately after it. The client puts `Accept-Signature` inside the encrypted request. On the response path, secure transport encrypts the application response first; RFC 9421 then signs the real ciphertext HTTP representation. The WASM client verifies the signature and digest before AES-GCM decryption and releases plaintext only after both layers and all request/session bindings succeed.

The combined runnable implementation is in [`examples/secure_wasm`](../examples/secure_wasm). Production builds embed the expected origin, X25519 transport public key, and Ed25519 signing public key directly in WASM; runtime configuration cannot substitute them. The WASM artifact hash must still be distributed through an independently authenticated release channel.
