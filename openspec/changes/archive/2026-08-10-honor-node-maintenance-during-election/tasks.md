## 1. Lock Down Maintenance Classification

- [x] 1.1 Add table-driven `ConsulElectionInteraction` tests for node maintenance, configured-service maintenance, configured critical and warning checks, ignored ordinary checks, and passing health.
- [x] 1.2 Add classification tests proving maintenance bypasses a non-empty `serviceChecks` allowlist while ordinary checks do not.
- [x] 1.3 Add tests for Agent health lookup errors and a missing configured local service so absence remains distinguishable from maintenance.

## 2. Read Instance-Scoped Local Health

- [x] 2.1 Extend the local Agent test seam and mocks to return service-ID-scoped health details from Consul.
- [x] 2.2 Replace the service-name catalog check lookup in `ConsulElectionInteraction.HealthState` with the local Agent health lookup for the configured service ID.
- [x] 2.3 Implement maintenance-first classification using Consul's node and service maintenance identifiers, then apply existing service-ID and `serviceChecks` filtering to ordinary checks.
- [x] 2.4 Preserve existing warning, passing, transport-error, and missing-service behavior through the new response shape.

## 3. Enforce Election Ineligibility

- [x] 3.1 Add election-step tests proving maintained followers do not create sessions, attempt KV acquisition, gain leadership, or receive the primary tag.
- [x] 3.2 Add election-step tests proving leaders under node or service maintenance release their sessions, become non-leaders, remove the primary tag, and return the existing critical-health result.
- [x] 3.3 Make maintenance and configured critical health use one pre-election relinquishment path before service lookup, session creation, and KV acquisition.
- [x] 3.4 Make relinquishment fail closed so a session-destroy error is reported without retaining local leadership or the primary tag.
- [x] 3.5 Add a recovery test proving an instance can participate on the next election step after maintenance is disabled and configured checks are eligible.

## 4. Verify Real Consul Behavior

- [x] 4.1 Add integration coverage that enables node maintenance on the local Consul agent and verifies a follower cannot be elected and a leader steps down.
- [x] 4.2 Add equivalent integration coverage for service maintenance, including maintenance precedence over `serviceChecks`.
- [x] 4.3 Verify disabling each maintenance mode restores election eligibility, with test cleanup that always disables maintenance and removes temporary sessions, services, and KV data.
- [x] 4.4 Where the integration harness supports ACLs, verify the service-ID health lookup exposes local maintenance under the token policy Ballot documents or uses in production.

## 5. Documentation and Validation

- [x] 5.1 Update the health-check documentation to state that local node and service maintenance always override configured election checks.
- [x] 5.2 Run `go test ./internal/ballot` and fix any focused unit-test failures.
- [x] 5.3 Run `go test -race ./...` and `go vet ./...`.
- [x] 5.4 Run `make test-integration` against the repository's isolated Consul setup and record any environment-dependent skips.

  Note: `make test-integration` currently fails at the `integration-up` step because the pre-existing `configs/integration/docker-compose.yaml` command (`agent -dev ...`) is incompatible with the current `hashicorp/consul:latest` entrypoint, which requires the `consul` binary name (the container exits with `exec: agent: not found`). This is a pre-existing config issue unrelated to this change. The integration suite was verified by starting the same Consul dev agent manually (`consul agent -dev -bind=0.0.0.0 -client=0.0.0.0`) and running `CONSUL_HTTP_ADDR=http://localhost:8500 go test -tags=integration -race ./...`: all integration tests pass, with `TestIntegration_Maintenance_ExposesLocalMaintenanceUnderACLToken` skipping because the dev agent does not enable ACLs.
