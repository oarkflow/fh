# Production security and operations evidence

These gates require evidence from the target environment. A passing unit or
loopback test, a harness in the repository, or a successful build does not
close them. Keep the run record, configuration revision, participant names,
UTC timestamps, raw logs and follow-up issues with the release evidence.

## MIRA independent review and penetration test

MIRA remains controlled-staging only until an independent reviewer has
completed a written protocol and implementation review. The engagement must
cover the custom transport cryptography, key exchange and rotation, replay and
resumption, downgrade handling, identity and authorization boundaries, parser
and resource limits, side channels, and the threat model for lost or
compromised devices. The report must identify the reviewed commit and build,
scope, tools and assumptions, findings with severity, and retest status.

Run an independent penetration test against a production-like deployment after
the cryptographic review fixes are merged. Include authenticated and
unauthenticated paths, gateway and device identities, revocation propagation,
multi-tenant isolation, malformed and fragmented traffic, denial-of-service
limits, secrets and administrative interfaces, and recovery after node or
network failure. Record the tested commit, deployment topology, findings,
evidence, remediation owners and retest outcomes. Do not report either gate as
passed until the signed reports and retest evidence are available.

## WAN and path-MTU validation

Use `scripts/soak-v7.sh` from a controlled host against staging endpoints over
real WAN paths. Run a minimum 24-hour soak on each supported path class and
repeat over IPv4 and IPv6 where supported. Include cellular, NAT rebinding,
loss, reordering, variable RTT, and reduced MTUs. Capture endpoint and client
versions, interface and route MTU, packet captures, network impairment
settings, CPU/RSS, reconnects, loss and recovery statistics, and the complete
JSONL output. Verify packet sizes and fragmentation behavior at multiple
path-MTU boundaries; loopback success is not path-MTU evidence. Define
acceptance thresholds before the run and attach an analysis of every failure.

## Multi-node PostgreSQL failover

Run the real adapter and application against at least three independent
application nodes and the production PostgreSQL topology, using the actual
driver, pool settings, TLS, schema and role permissions. Exercise primary
failure, promotion, stale connections, partition and recovery, restart during
transactions, concurrent idempotency keys, and outbox delivery to an effect
that records deduplication. Verify no duplicate externally visible effect,
no lost acknowledged transaction, bounded recovery time, and correct behavior
when the database is unavailable. Save database and application logs, topology,
configuration, exact software revisions and measured recovery results.

The in-memory three-node lease test is not evidence for PostgreSQL failover.
The SQL adapter is not declared production-ready for a deployment until these
results pass the deployment's predeclared consistency and recovery objectives.

## SQL queue replica claims and takeover

Run the SQL queue contract against the supported PostgreSQL and MySQL server
versions using the production driver, connection-pool limits, TLS, schema,
database roles and at least three independent worker processes. Use the real
shared database; independent handles in one process are useful concurrency
coverage, but do not replace this drill. Preserve the exact server and driver
versions, migration revision, lease TTL, worker settings, commands, UTC event
timeline and database/application logs.

For each backend, enqueue a batch of uniquely identified jobs and start more
workers than jobs. Confirm every job is claimed once at a time, scheduled jobs
are not claimed early, and database outages do not cause false success. Hold a
claimed handler past its lease expiry while another worker reclaims the job;
then let the old worker attempt renewal, completion, failure and retry. Each
stale transition must be rejected, and only the latest fence may change queue
state. Repeat with a worker process killed after claim and with database
connections interrupted during claim, renewal and transition. Exercise
concurrency-key leases on multiple replicas and verify expired owners can be
replaced without allowing a stale fence to release or overwrite the new owner.

For PostgreSQL, repeat during primary promotion and application-side network
partition. For MySQL, repeat with the production InnoDB topology during primary
failure, promotion and stale-connection recovery. Verify migrations complete
before the deployment gate advertises multi-replica support, and that
non-InnoDB queue tables fail closed. Track job IDs and fences in the evidence;
check for duplicate claims, stuck processing rows, lost acknowledgements,
lease-expiry recovery time and duplicate external effects. Since fencing the
queue row cannot fence an arbitrary handler side effect, make that effect
idempotent or have its transactional store reject stale fences. Record the
measured recovery and duplicate-effect results against objectives defined
before the drill. Do not use these results to close the broader database
failover gate unless that gate's topology and scenarios were also exercised.

## Failover and rollback exercises

Before each production release, rehearse both procedures in a production-like
environment:

1. Fail over one application node and one stateful dependency while traffic is
   active. Verify health checks, shared-state behavior, in-flight request
   outcomes, alerting and recovery against predeclared objectives.
2. Roll back to the prior release using the documented deployment mechanism.
   Verify schema compatibility, client compatibility, secrets/configuration,
   health and traffic restoration. Include a rollback after a failed migration
   where the migration policy permits it.
3. Restore the recovered topology and confirm there are no stale owners,
   duplicate jobs, stuck idempotency records or missed outbox effects.

Capture operator commands, UTC timeline, dashboards and alerts, error rates,
recovery time, data-integrity checks, deviations and assigned remediation.
The incident commander records a pass only when all agreed service and data
objectives are met and follow-up actions have owners.

## Long soaks

Run a minimum 72-hour soak for each production transport and representative
workload before declaring the release stable. Include idle keep-alive, churn,
HTTP/2 streams, uploads, WebSocket/SSE if used, and overload recovery. Capture
latency percentiles, throughput, CPU, RSS, goroutine and descriptor counts,
GC metrics, restarts, errors and dependency health over time. Establish
workload-specific thresholds before the run, retain raw data, and explain
trends or outliers. The MIRA WAN soak script does not cover the framework's
HTTP transports or prove a successful run by itself.
