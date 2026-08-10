## ADDED Requirements

### Requirement: Local maintenance shall make an instance ineligible for election
Ballot SHALL treat maintenance of either the local Consul node or the configured local service instance as an unconditional election blocker before creating or reusing leadership state.

#### Scenario: Follower node is in maintenance
- **WHEN** an election step runs while the local Consul node is in maintenance
- **THEN** Ballot remains non-leader and does not create a session or attempt to acquire the KV lock

#### Scenario: Follower service is in maintenance
- **WHEN** an election step runs while the configured local service instance is in maintenance
- **THEN** Ballot remains non-leader and does not create a session or attempt to acquire the KV lock

#### Scenario: Maintenance check is absent from configured checks
- **WHEN** local node or service maintenance is active and the generated maintenance check is not listed in `serviceChecks`
- **THEN** Ballot still treats the instance as ineligible for election

### Requirement: A maintained leader shall relinquish leadership
Ballot SHALL make a leader that enters local node or service maintenance release its election session, clear local leadership, remove its primary tag, and stop the election step before any acquisition attempt.

#### Scenario: Leader node enters maintenance
- **WHEN** the local Consul node enters maintenance before the current leader's next election step
- **THEN** Ballot releases the leader session, marks the instance non-leader, removes its primary tag, and returns an ineligible-health result

#### Scenario: Leader service enters maintenance
- **WHEN** the configured local service instance enters maintenance before the current leader's next election step
- **THEN** Ballot releases the leader session, marks the instance non-leader, removes its primary tag, and returns an ineligible-health result

#### Scenario: Session release fails during maintenance
- **WHEN** Ballot detects local maintenance but Consul fails to destroy the active session
- **THEN** Ballot still clears local leadership and attempts to remove the primary tag while reporting the session-release failure

### Requirement: Maintenance evaluation shall remain instance-scoped
Ballot SHALL base maintenance eligibility on the configured local service instance and its local Consul node without allowing maintenance elsewhere to disqualify it.

#### Scenario: Another node is in maintenance
- **WHEN** a different Consul node hosting another service instance is in maintenance and the configured local instance is eligible
- **THEN** Ballot allows the configured local instance to participate in election

#### Scenario: Another service instance is in maintenance
- **WHEN** another instance of the same service is in maintenance and the configured local instance is eligible
- **THEN** Ballot allows the configured local instance to participate in election

### Requirement: Ordinary health-check filtering shall be preserved
Ballot SHALL continue to evaluate ordinary service checks using the configured service ID and optional `serviceChecks` allowlist after maintenance has been ruled out.

#### Scenario: Unconfigured ordinary check is critical
- **WHEN** an ordinary check for the configured service instance is critical but is excluded by a non-empty `serviceChecks` allowlist
- **THEN** that check does not prevent the instance from participating in election

#### Scenario: Configured ordinary check is critical
- **WHEN** an ordinary check for the configured service instance is critical and is included in `serviceChecks`
- **THEN** Ballot applies the existing critical-health relinquishment behavior

#### Scenario: Check belongs to another service instance
- **WHEN** an ordinary critical check belongs to a different service instance
- **THEN** that check does not prevent the configured local instance from participating in election

### Requirement: Election eligibility shall recover after maintenance
Ballot SHALL reevaluate Consul maintenance on each election step without persisting a separate local maintenance state.

#### Scenario: Maintenance is disabled
- **WHEN** local maintenance is disabled and the configured service checks otherwise permit election
- **THEN** Ballot may create or reuse a session and attempt to acquire leadership on the next election step

### Requirement: Missing local service shall remain a service lookup failure
Ballot SHALL distinguish absence of the configured local service from maintenance so the election step preserves its existing service-failure result.

#### Scenario: Configured service does not exist
- **WHEN** Consul has no local service matching the configured service ID
- **THEN** the election step reports a service lookup failure and does not report maintenance
