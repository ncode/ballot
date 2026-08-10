## Why

Ballot currently evaluates only service-scoped health checks during an election, while Consul exposes node maintenance as a node-scoped check. A node placed in maintenance can therefore acquire or retain leadership, defeating the expectation that maintenance removes that node's services from active responsibility.

## What Changes

- Treat maintenance of the local Consul node as an unconditional reason for the configured service instance not to participate in leader election.
- Give local service maintenance the same unconditional behavior, independent of the configured `serviceChecks` allowlist.
- Make an instance that enters maintenance release its election session, clear local leadership, and remove its primary tag through the existing unhealthy-instance transition.
- Prevent an instance already in maintenance from creating a session or attempting the KV lock.
- Preserve existing filtering for ordinary service checks and ignore health or maintenance state belonging only to other instances or nodes.
- Allow the instance to participate again on a later election step after maintenance is disabled and its configured health checks permit election.

## Capabilities

### New Capabilities

- `election-maintenance-eligibility`: Defines how local node and service maintenance affect election eligibility, leadership relinquishment, and later re-entry.

### Modified Capabilities

- None. This repository has no existing main OpenSpec specifications.

## Impact

- Affected code is expected in `internal/ballot/consul_interaction.go`, `internal/ballot/election_step.go`, and their unit-test seams.
- The Consul health lookup must expose node maintenance for the configured local service ID while preserving configured service-check filtering.
- Integration coverage should exercise both node and service maintenance against a real Consul agent.
- No configuration changes, public API changes, or new production dependencies are expected.
