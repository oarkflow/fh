# Guide-compatible v7 source fix

The original source erroneously validates empty gateway delegation flags as an error.
In `cmd/mira-tls-server/main.go`, the expression checking whether gateway identity and trusted signing keys are configured together has been corrected from `!=` to `==`.
This leaves the wire revision at 4 and permits server startup with no browser gateway.

Validated with a separate-process mTLS client/server echo on localhost and Go transport tests.
