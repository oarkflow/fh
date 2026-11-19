# MIRA-T/1 v7 verification record

> Imported from the MIRA reference tree. The results below were recorded there;
> they are not evidence about this fh checkout. fh's post-port verification is
> described in `docs/mira/transport.md`.

Run `bash scripts/verify-v7.sh` in the source root to reproduce.

A local test suite completed on 2026-11-08 covering `go build ./...`, `go vet ./...`, `go test ./... -count=1`, race-enabled tests for `delegation`, `gateway`, `mira`, `resume`, `cluster`, repeated simulated loss/migration/IPv6 tests, independent Python wire fixtures, real encrypted Python-client/Go-server interoperability, and browser TypeScript compilation. The log is included in the delivered release workspace; results depend on runtime and environment.

The **v7 named delegation end-to-end** test was added after the long verification script completed and separately passed with `go test -race ./delegation -run TestNamedDelegationAcrossRealEncryptedUDPChannel`.

All tests use local or simulated conditions. They are not a cryptographic audit or published real-world performance report. The `soak-v7.sh` command is an operator tool, not evidence that a WAN soak ran.

## Final regression correction

During a final run, the added named-delegation end-to-end test sometimes closed its server-side session before the transport acknowledgment arrived. The test was updated to await its transport ACK before server shutdown. After the correction, this test passed 20 normal runs and 10 race-enabled runs. A subsequent complete `go test ./... -count=1 -timeout 100s` run passed, followed by the race-enabled delegation/gateway/resume/cluster test run. No claim is made that this eliminates unrelated transport stalls or that test execution certifies the protocol.
