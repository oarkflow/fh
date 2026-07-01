# mtls middleware

Validates verified client certificate chains for admin APIs, internal routes, and high-trust control plane calls. Use `Required` to reject requests without a certificate, and use subject/issuer allowlists for tighter service identity controls.

Impact: this blocks unauthenticated clients before business handlers run. Do not trust client-certificate headers unless they come from a trusted TLS-terminating proxy.
