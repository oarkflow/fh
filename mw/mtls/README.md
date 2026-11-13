# mtls middleware

Validates verified client certificate chains for admin APIs, internal routes, and high-trust control plane calls. Use `Required` to reject requests without a certificate, and use subject/issuer allowlists for tighter service identity controls.

Impact: this blocks unauthenticated clients before business handlers run. Do not trust client-certificate headers unless they come from a trusted TLS-terminating proxy.

`AllowUnverified` is accepted only when `Verify` performs full certificate
validation. Certificates supplied through `WithPeerCertificates` also require
`Verify`; raw certificates attached to a context do not prove identity.
Malformed chains containing nil certificate entries are rejected before
identity checks or verifier callbacks run.
