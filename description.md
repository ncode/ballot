# Honor Consul maintenance during leader election

## Summary

Prevent Ballot instances from participating in leader election while their
local Consul node or service is in maintenance.

## Problem

The catalog health-check endpoint did not expose local node maintenance for a
service. As a result, a maintained instance could continue acquiring
leadership, or remain leader after maintenance was enabled.

## Changes

- Use Consul's local Agent health lookup scoped to the configured service ID.
- Detect node maintenance and service maintenance before applying the
  configured `serviceChecks` allowlist.
- Prevent maintained followers from creating sessions or acquiring the KV
  lock.
- Make maintained leaders release their session, clear local leadership, and
  remove the primary tag.
- Fail closed when session destruction or tag removal fails.
- Restore election eligibility after maintenance is disabled.
- Add unit, integration, and ACL-aware coverage for the new behavior.
- Fix the integration Compose command to invoke `consul agent` explicitly.

## Validation

- `go test ./internal/ballot -run TestHandleServiceCriticalState_ErrorPaths -count=1`
- `make test-coverage`
- `make test-integration`

The integration suite passes locally. The ACL-specific integration test skips
when the development Consul agent has ACLs disabled.

## Coverage

The initial branch coverage report was **96.2%**, compared with
**96.3%** on `origin/main`. The decrease came from an uncovered combined error
path when both session release and leadership status update fail.

A focused regression test now covers that path. The exact CI coverage command
reports **96.4%**, above the `origin/main` baseline. No Codecov thresholds or
coverage settings were changed.
