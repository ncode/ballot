## Context

Each election step currently starts by asking `ConsulElectionInteraction.HealthState` for the configured service's health. That method calls Consul's catalog-oriented `Health.Checks(serviceName)` endpoint, filters checks to the configured service ID and optional `serviceChecks` allowlist, and blocks election only for a resulting critical state.

This preserves instance and configured-check isolation, but it cannot observe node maintenance. Consul's service-check endpoint omits the node-scoped `_node_maintenance` check. Service maintenance is returned, but its generated check can also be discarded by the configured-check allowlist. As a result, a maintained instance can create a session, acquire the KV lock, or continue to advertise the primary tag.

Ballot already assumes that its configured Consul address reaches the local agent: local service lookup and tag updates use Agent APIs with the configured service ID. The change must preserve that assumption, existing configuration, normal service-check filtering, and the ordered election-step surface.

## Goals / Non-Goals

**Goals:**

- Detect maintenance of the local node and configured local service instance before session creation or lock acquisition.
- Make node and service maintenance unconditional election blockers, independent of `serviceChecks`.
- Relinquish an existing leader's session and primary tag when maintenance begins.
- Preserve instance isolation and the allowlist behavior for ordinary service health checks.
- Keep maintenance failures visible through the existing election-step failure surface.
- Cover the Consul response shape in focused unit tests and real-agent integration tests.

**Non-Goals:**

- Change warning-state behavior or make every Consul check election-critical.
- React to maintenance or health state belonging only to another node or service instance.
- Add configuration for opting out of maintenance handling.
- Change leadership hooks, lock format, session TTL behavior, or preferred-leader selection.
- Add a new public election-step status solely for maintenance.

## Decisions

### 1. Read local health by service ID

`ConsulElectionInteraction` will obtain health details from the local Agent health endpoint for the configured service ID. The response identifies exactly one local service and includes its service checks plus local node maintenance.

This is preferred over:

- `Health.Checks(serviceName)`, which does not return node-scoped checks.
- `Health.Service(serviceName, ...)`, which returns node checks but queries the catalog, returns all instances, and can require additional node-read ACL visibility.
- `Agent.Checks()`, which returns unrelated local checks and can hide node-scoped checks from service-only ACL tokens.
- `AgentHealthServiceByName`, which aggregates every same-name service registered on the local agent and would weaken service-ID isolation.

The ID-scoped Agent health endpoint returned node maintenance for the configured service under a service-read-only token in the investigation, so no additional Consul ACL grant is expected.

If Consul reports no service details for the configured ID, health evaluation will not convert that absence into maintenance. The existing local-service lookup remains responsible for producing the election step's service-failure result.

### 2. Classify maintenance before applying the configured-check allowlist

Health classification will use this precedence:

```text
local node or service maintenance
              >
configured critical service check
              >
configured warning service check
              >
passing
```

The local node maintenance identifier and the maintenance identifier for the configured service ID are control-plane signals, not user-selected application checks. They will therefore be considered before `serviceChecks` filtering. Ordinary checks will continue to require the configured service ID and, when non-empty, membership in `serviceChecks`.

The implementation must inspect the response's aggregated status or maintenance check identifiers rather than relying only on the Agent health method's returned status string. Consul transports maintenance through a service-unavailable response that the Go client reports as critical even though the response body identifies the aggregate state as maintenance.

### 3. Reuse the existing pre-election ineligibility transition

Both maintenance kinds will follow the same ordered path already used for critical configured health:

```text
detect ineligible health
        |
        v
release active session
        |
        v
mark local instance non-leader
        |
        v
remove primary tag
        |
        v
return before service/session/KV acquisition
```

The election step will continue to report `ElectionStepCriticalHealth` for this pre-election rejection. This avoids expanding the public result model while preserving an observable failure that callers already handle.

Relinquishment is fail-closed. If session destruction fails, Ballot must still clear its local leadership state and attempt to remove the primary tag, while returning the operational error. The Consul lock may remain unavailable until its session expires, but the maintained instance must not continue presenting itself as eligible or primary.

### 4. Re-entry is evaluated, not persisted

Ballot will not store a separate maintenance flag. Every election step will evaluate current Consul health. After maintenance is disabled, an instance can participate on the next step if its configured checks otherwise permit election.

This avoids stale local state and requires no migration or synchronization mechanism.

### 5. Verify behavior at interaction, election-step, and integration boundaries

Focused interaction tests will classify node maintenance, service maintenance, configured critical checks, ignored checks, and missing service details. Election-step tests will prove that maintained followers do not call session or KV APIs and that maintained leaders execute relinquishment. Integration tests will toggle both Consul maintenance modes against a real local agent and verify loss and later recovery of election eligibility.

## Risks / Trade-offs

- **Consul versions may encode Agent health responses differently** → Detect the documented maintenance identifiers, retain aggregate-state coverage, and exercise the repository's supported Consul version in integration tests.
- **The Agent health endpoint returns HTTP service-unavailable for unhealthy states** → Use the Consul Go client's structured response and do not treat that status alone as a transport failure.
- **Session destruction can fail while the KV lock remains held** → Clear local leadership and primary routing state, report the error, and rely on Consul session expiry before another instance acquires the lock.
- **Changing the health source could accidentally broaden ordinary check handling** → Keep explicit unit cases for the configured-check allowlist, other service IDs, warning state, and maintenance precedence.
- **A token may expose different check details under ACL filtering** → Retain service-ID-scoped lookup and include an ACL-restricted integration case when the test harness can support it.

## Migration Plan

No data or configuration migration is required. Deploy the updated binary normally; maintenance-aware eligibility takes effect on the first election step after deployment. Rollback consists of restoring the previous binary, with existing Consul sessions and KV data remaining compatible.

## Open Questions

None.
