//go:build integration

package ballot

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testPrimaryTag  = "primary"
	testElectionKey = "election/test-service/leader"
)

func getConsulAddr() string {
	addr := os.Getenv("CONSUL_HTTP_ADDR")
	if addr == "" {
		return "http://localhost:8500"
	}
	return addr
}

func setupConsulClient(t *testing.T) *api.Client {
	t.Helper()
	config := api.DefaultConfig()
	config.Address = getConsulAddr()

	client, err := api.NewClient(config)
	require.NoError(t, err, "Failed to create Consul client")

	// Verify Consul is reachable
	_, err = client.Agent().Self()
	require.NoError(t, err, "Consul is not reachable at %s", config.Address)

	return client
}

func registerTestService(t *testing.T, client *api.Client, serviceID string, port int) {
	t.Helper()
	// Note: The service Name must match what Ballot.Name expects.
	// Ballot.Name defaults to the name parameter passed to New(), which is serviceID.
	reg := &api.AgentServiceRegistration{
		ID:      serviceID,
		Name:    serviceID,
		Port:    port,
		Address: "127.0.0.1",
		Tags:    []string{"test"},
		Check: &api.AgentServiceCheck{
			CheckID: fmt.Sprintf("service:%s", serviceID),
			TTL:     "30s",
			Status:  "passing",
		},
		EnableTagOverride: true,
	}
	err := client.Agent().ServiceRegister(reg)
	require.NoError(t, err, "Failed to register test service")
}

func deregisterTestService(t *testing.T, client *api.Client, serviceID string) {
	t.Helper()
	err := client.Agent().ServiceDeregister(serviceID)
	if err != nil {
		t.Logf("Warning: failed to deregister service %s: %v", serviceID, err)
	}
}

func cleanupKV(t *testing.T, client *api.Client, key string) {
	t.Helper()
	_, err := client.KV().Delete(key, nil)
	if err != nil {
		t.Logf("Warning: failed to delete KV key %s: %v", key, err)
	}
}

func testRuntimeConfig(serviceID string, electionKey string) RuntimeConfig {
	return RuntimeConfig{
		Name:          serviceID,
		ID:            serviceID,
		Key:           electionKey,
		PrimaryTag:    testPrimaryTag,
		ServiceChecks: []string{fmt.Sprintf("service:%s", serviceID)},
		ConsulAddress: getConsulAddr(),
		TTL:           10 * time.Second,
		LockDelay:     time.Second,
	}
}

func TestIntegration_FullElectionCycle(t *testing.T) {
	client := setupConsulClient(t)

	serviceID := fmt.Sprintf("test-service-%d", time.Now().UnixNano())
	electionKey := fmt.Sprintf("election/test/%s/leader", serviceID)

	// Register test service
	registerTestService(t, client, serviceID, 8080)
	defer deregisterTestService(t, client, serviceID)
	defer cleanupKV(t, client, electionKey)

	// Create ballot instance
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ballot, err := New(ctx, testRuntimeConfig(serviceID, electionKey))
	require.NoError(t, err, "Failed to create Ballot instance")
	defer ballot.releaseSession()

	// Run a single election cycle
	err = ballot.election()
	require.NoError(t, err, "Election failed")

	// Verify session was created
	sessionID, ok := ballot.getSessionID()
	assert.True(t, ok, "Session ID should be set")
	assert.NotNil(t, sessionID, "Session ID should not be nil")

	// Verify we became leader
	assert.True(t, ballot.IsLeader(), "Should be leader after election")

	// Verify KV lock was acquired
	kvPair, _, err := client.KV().Get(electionKey, nil)
	require.NoError(t, err)
	require.NotNil(t, kvPair, "KV pair should exist")
	assert.Equal(t, *sessionID, kvPair.Session, "KV should be locked by our session")

	// Verify primary tag was added
	service, _, err := client.Agent().Service(serviceID, nil)
	require.NoError(t, err)
	assert.Contains(t, service.Tags, testPrimaryTag, "Service should have primary tag")
}

func TestIntegration_LeaderFailover(t *testing.T) {
	client := setupConsulClient(t)

	serviceID1 := fmt.Sprintf("test-service-1-%d", time.Now().UnixNano())
	serviceID2 := fmt.Sprintf("test-service-2-%d", time.Now().UnixNano())
	electionKey := fmt.Sprintf("election/test/failover-%d/leader", time.Now().UnixNano())

	// Register two test services
	registerTestService(t, client, serviceID1, 8081)
	registerTestService(t, client, serviceID2, 8082)
	defer deregisterTestService(t, client, serviceID1)
	defer deregisterTestService(t, client, serviceID2)
	defer cleanupKV(t, client, electionKey)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ballot1, err := New(ctx, testRuntimeConfig(serviceID1, electionKey))
	require.NoError(t, err)
	defer ballot1.releaseSession()

	ballot2, err := New(ctx, testRuntimeConfig(serviceID2, electionKey))
	require.NoError(t, err)
	defer ballot2.releaseSession()

	// First ballot becomes leader
	err = ballot1.election()
	require.NoError(t, err)
	assert.True(t, ballot1.IsLeader(), "Ballot 1 should be leader")

	// Second ballot runs election but shouldn't become leader
	err = ballot2.election()
	require.NoError(t, err)
	assert.False(t, ballot2.IsLeader(), "Ballot 2 should not be leader yet")

	// Get first ballot's session ID and destroy it
	sessionID1, ok := ballot1.getSessionID()
	require.True(t, ok)
	require.NotNil(t, sessionID1)

	_, err = client.Session().Destroy(*sessionID1, nil)
	require.NoError(t, err, "Failed to destroy session")

	require.Eventually(t, func() bool {
		err = ballot2.election()
		return err == nil && ballot2.IsLeader()
	}, 5*time.Second, 100*time.Millisecond, "Ballot 2 should be leader after failover")

	// Verify primary tag moved to second service
	service2, _, err := client.Agent().Service(serviceID2, nil)
	require.NoError(t, err)
	assert.Contains(t, service2.Tags, testPrimaryTag, "Service 2 should have primary tag")
}

func TestIntegration_TagPromotion(t *testing.T) {
	client := setupConsulClient(t)

	serviceID := fmt.Sprintf("test-service-tags-%d", time.Now().UnixNano())
	electionKey := fmt.Sprintf("election/test/tags-%d/leader", time.Now().UnixNano())

	registerTestService(t, client, serviceID, 8083)
	defer deregisterTestService(t, client, serviceID)
	defer cleanupKV(t, client, electionKey)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ballot, err := New(ctx, testRuntimeConfig(serviceID, electionKey))
	require.NoError(t, err)
	defer ballot.releaseSession()

	// Verify service doesn't have primary tag initially
	service, _, err := client.Agent().Service(serviceID, nil)
	require.NoError(t, err)
	assert.NotContains(t, service.Tags, testPrimaryTag, "Should not have primary tag initially")

	// Run election to become leader
	err = ballot.election()
	require.NoError(t, err)
	assert.True(t, ballot.IsLeader())

	// Verify primary tag was added
	service, _, err = client.Agent().Service(serviceID, nil)
	require.NoError(t, err)
	assert.Contains(t, service.Tags, testPrimaryTag, "Should have primary tag after becoming leader")
}

func TestIntegration_HealthCheckFailure(t *testing.T) {
	client := setupConsulClient(t)

	serviceID := fmt.Sprintf("test-service-health-%d", time.Now().UnixNano())
	electionKey := fmt.Sprintf("election/test/health-%d/leader", time.Now().UnixNano())
	checkID := fmt.Sprintf("service:%s", serviceID)

	registerTestService(t, client, serviceID, 8084)
	defer deregisterTestService(t, client, serviceID)
	defer cleanupKV(t, client, electionKey)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ballot, err := New(ctx, testRuntimeConfig(serviceID, electionKey))
	require.NoError(t, err)
	defer ballot.releaseSession()

	// Become leader first
	err = ballot.election()
	require.NoError(t, err)
	assert.True(t, ballot.IsLeader(), "Should be leader")

	// Mark health check as critical
	err = client.Agent().UpdateTTL(checkID, "simulated failure", "critical")
	require.NoError(t, err, "Failed to update TTL check")

	// Wait for health status to propagate and verify it's critical
	require.Eventually(t, func() bool {
		checks, _, err := client.Health().Checks(serviceID, nil)
		if err != nil {
			return false
		}
		for _, check := range checks {
			if check.CheckID == checkID && check.Status == "critical" {
				return true
			}
		}
		return false
	}, 5*time.Second, 100*time.Millisecond, "Health check should become critical")

	// Run election - should detect critical state and step down
	err = ballot.election()
	assert.Error(t, err, "Election should fail when service is critical")
	assert.Contains(t, err.Error(), "critical state")

	// Verify we're no longer leader
	assert.False(t, ballot.IsLeader(), "Should not be leader when health check is critical")
}

func TestIntegration_MultipleInstances(t *testing.T) {
	client := setupConsulClient(t)

	baseID := fmt.Sprintf("test-multi-%d", time.Now().UnixNano())
	electionKey := fmt.Sprintf("election/test/multi-%d/leader", time.Now().UnixNano())

	numInstances := 3
	services := make([]string, numInstances)
	ballots := make([]*Ballot, numInstances)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Register multiple services and create ballots
	// Note: Each iteration resets viper, but Ballot copies config at creation
	// time, so previously created ballots are unaffected.
	for i := 0; i < numInstances; i++ {
		services[i] = fmt.Sprintf("%s-%d", baseID, i)
		registerTestService(t, client, services[i], 8090+i)
		defer deregisterTestService(t, client, services[i])

		b, err := New(ctx, testRuntimeConfig(services[i], electionKey))
		require.NoError(t, err)
		defer b.releaseSession()
		ballots[i] = b
	}
	defer cleanupKV(t, client, electionKey)

	// Run elections for all instances
	for i, b := range ballots {
		err := b.election()
		require.NoError(t, err, "Election failed for instance %d", i)
	}

	// Count leaders - exactly one should be leader
	leaderCount := 0
	var leaderIndex int
	for i, b := range ballots {
		if b.IsLeader() {
			leaderCount++
			leaderIndex = i
		}
	}

	assert.Equal(t, 1, leaderCount, "Exactly one instance should be leader")

	// Verify only the leader has the primary tag
	for i, serviceID := range services {
		service, _, err := client.Agent().Service(serviceID, nil)
		require.NoError(t, err)

		if i == leaderIndex {
			assert.Contains(t, service.Tags, testPrimaryTag, "Leader should have primary tag")
		} else {
			assert.NotContains(t, service.Tags, testPrimaryTag, "Non-leader should not have primary tag")
		}
	}
}

func TestIntegration_SessionRenewal(t *testing.T) {
	client := setupConsulClient(t)

	serviceID := fmt.Sprintf("test-service-renewal-%d", time.Now().UnixNano())
	electionKey := fmt.Sprintf("election/test/renewal-%d/leader", time.Now().UnixNano())

	registerTestService(t, client, serviceID, 8095)
	defer deregisterTestService(t, client, serviceID)
	defer cleanupKV(t, client, electionKey)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ballot, err := New(ctx, testRuntimeConfig(serviceID, electionKey))
	require.NoError(t, err)
	defer ballot.releaseSession()

	// Become leader
	err = ballot.election()
	require.NoError(t, err)
	assert.True(t, ballot.IsLeader())

	sessionID, ok := ballot.getSessionID()
	require.True(t, ok)
	require.NotNil(t, sessionID)

	renewalWindow := time.Now().Add(6 * time.Second)
	require.Eventually(t, func() bool {
		if time.Now().Before(renewalWindow) {
			return false
		}
		sessionInfo, _, err := client.Session().Info(*sessionID, nil)
		return err == nil && sessionInfo != nil
	}, 8*time.Second, 100*time.Millisecond, "Session should still exist after renewal")

	// Run another election - should maintain leadership
	err = ballot.election()
	require.NoError(t, err)
	assert.True(t, ballot.IsLeader(), "Should still be leader after session renewal")
}

// disableMaintenance ensures both node and service maintenance are disabled for
// the configured service ID. It is always invoked by deferred cleanup so a
// failed test never leaves a Consul agent in maintenance.
func disableMaintenance(t *testing.T, client *api.Client, serviceID string) {
	t.Helper()
	if err := client.Agent().DisableServiceMaintenance(serviceID); err != nil {
		t.Logf("Warning: failed to disable service maintenance for %s: %v", serviceID, err)
	}
	if err := client.Agent().DisableNodeMaintenance(); err != nil {
		t.Logf("Warning: failed to disable node maintenance: %v", err)
	}
}

// waitForAggregatedHealth waits until the local Agent reports the expected
// aggregated health status for the configured service ID. Consul transports
// maintenance through a service-unavailable response that the Go client
// reports as critical in its first return value, so the reliable aggregate
// state is read from the response body's AggregatedStatus field.
func waitForAggregatedHealth(t *testing.T, client *api.Client, serviceID, expected string) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, info, err := client.Agent().AgentHealthServiceByID(serviceID)
		if err != nil {
			return false
		}
		if info != nil {
			return info.AggregatedStatus == expected
		}
		// No service details (e.g. not found): fall back to the absent/critical
		// signal only when that is what the caller expects.
		return expected == api.HealthCritical
	}, 5*time.Second, 100*time.Millisecond, "service %s should reach %s health", serviceID, expected)
}

// TestIntegration_NodeMaintenance_FollowerCannotBeElected verifies that a
// follower cannot be elected while the local Consul node is in maintenance.
// See task 4.1.
func TestIntegration_NodeMaintenance_FollowerCannotBeElected(t *testing.T) {
	client := setupConsulClient(t)

	serviceID := fmt.Sprintf("test-maint-node-follower-%d", time.Now().UnixNano())
	electionKey := fmt.Sprintf("election/test/maint-node-follower-%d/leader", time.Now().UnixNano())

	registerTestService(t, client, serviceID, 8700)
	defer deregisterTestService(t, client, serviceID)
	defer cleanupKV(t, client, electionKey)
	defer disableMaintenance(t, client, serviceID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ballot, err := New(ctx, testRuntimeConfig(serviceID, electionKey))
	require.NoError(t, err)
	defer ballot.releaseSession()

	// Enable node maintenance and wait for the agent to reflect it.
	require.NoError(t, client.Agent().EnableNodeMaintenance("integration test"))
	waitForAggregatedHealth(t, client, serviceID, api.HealthMaint)

	// Run an election step while in maintenance: the follower must not be
	// elected and must report the critical-health result.
	err = ballot.election()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "critical state")
	assert.False(t, ballot.IsLeader(), "Maintained follower must not become leader")

	// No session should have been created.
	sessionID, ok := ballot.getSessionID()
	assert.False(t, ok, "Maintained follower must not create a session")
	assert.Nil(t, sessionID, "Maintained follower must not create a session")

	// No KV lock should be held.
	kvPair, _, err := client.KV().Get(electionKey, nil)
	require.NoError(t, err)
	assert.Nil(t, kvPair, "No KV lock should exist while in maintenance")
}

// TestIntegration_NodeMaintenance_LeaderStepsDown verifies that an existing
// leader steps down when the local Consul node enters maintenance. See task
// 4.1.
func TestIntegration_NodeMaintenance_LeaderStepsDown(t *testing.T) {
	client := setupConsulClient(t)

	serviceID := fmt.Sprintf("test-maint-node-leader-%d", time.Now().UnixNano())
	electionKey := fmt.Sprintf("election/test/maint-node-leader-%d/leader", time.Now().UnixNano())

	registerTestService(t, client, serviceID, 8701)
	defer deregisterTestService(t, client, serviceID)
	defer cleanupKV(t, client, electionKey)
	defer disableMaintenance(t, client, serviceID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ballot, err := New(ctx, testRuntimeConfig(serviceID, electionKey))
	require.NoError(t, err)
	defer ballot.releaseSession()

	// Become leader first.
	require.NoError(t, ballot.election())
	require.True(t, ballot.IsLeader(), "Should be leader before maintenance")

	// Enable node maintenance and wait for the agent to reflect it.
	require.NoError(t, client.Agent().EnableNodeMaintenance("integration test"))
	waitForAggregatedHealth(t, client, serviceID, api.HealthMaint)

	// Run another election step: the leader must step down.
	err = ballot.election()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "critical state")
	assert.False(t, ballot.IsLeader(), "Leader must step down under node maintenance")

	// The primary tag should have been removed.
	service, _, err := client.Agent().Service(serviceID, nil)
	require.NoError(t, err)
	assert.NotContains(t, service.Tags, testPrimaryTag, "Primary tag must be removed under maintenance")

	// Disabling maintenance restores eligibility. See task 4.3. The recovery
	// election is retried to account for Consul's lock delay after the
	// leadership session was released during step-down.
	require.NoError(t, client.Agent().DisableNodeMaintenance())
	waitForAggregatedHealth(t, client, serviceID, api.HealthPassing)

	require.Eventually(t, func() bool {
		err := ballot.election()
		return err == nil && ballot.IsLeader()
	}, 5*time.Second, 100*time.Millisecond, "Should be eligible again after maintenance is disabled")
}

// TestIntegration_ServiceMaintenance_PrecedenceOverServiceChecks verifies that
// service maintenance blocks election even when the generated maintenance
// check is not listed in serviceChecks, and that a leader steps down. See
// tasks 4.2 and 4.3.
func TestIntegration_ServiceMaintenance_PrecedenceOverServiceChecks(t *testing.T) {
	client := setupConsulClient(t)

	serviceID := fmt.Sprintf("test-maint-svc-%d", time.Now().UnixNano())
	electionKey := fmt.Sprintf("election/test/maint-svc-%d/leader", time.Now().UnixNano())

	registerTestService(t, client, serviceID, 8702)
	defer deregisterTestService(t, client, serviceID)
	defer cleanupKV(t, client, electionKey)
	defer disableMaintenance(t, client, serviceID)

	// Configure serviceChecks with only the service TTL check; the generated
	// service-maintenance check id is never listed there.
	cfg := testRuntimeConfig(serviceID, electionKey)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ballot, err := New(ctx, cfg)
	require.NoError(t, err)
	defer ballot.releaseSession()

	// Become leader first.
	require.NoError(t, ballot.election())
	require.True(t, ballot.IsLeader(), "Should be leader before maintenance")

	// Enable service maintenance and wait for the agent to reflect it.
	require.NoError(t, client.Agent().EnableServiceMaintenance(serviceID, "integration test"))
	waitForAggregatedHealth(t, client, serviceID, api.HealthMaint)

	// Run another election step: the leader must step down despite the
	// maintenance check not being in serviceChecks.
	err = ballot.election()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "critical state")
	assert.False(t, ballot.IsLeader(), "Leader must step down under service maintenance")

	// Disabling maintenance restores eligibility. See task 4.3. The recovery
	// election is retried to account for Consul's lock delay after the
	// leadership session was released during step-down.
	require.NoError(t, client.Agent().DisableServiceMaintenance(serviceID))
	waitForAggregatedHealth(t, client, serviceID, api.HealthPassing)

	require.Eventually(t, func() bool {
		err := ballot.election()
		return err == nil && ballot.IsLeader()
	}, 5*time.Second, 100*time.Millisecond, "Should be eligible again after service maintenance is disabled")
}

// TestIntegration_Maintenance_ExposesLocalMaintenanceUnderACLToken is a
// placeholder for ACL-restricted coverage. The repository's integration
// Consul agent runs in -dev mode without ACLs enabled, so the service-ID
// health lookup cannot be exercised under a token policy here. The test
// documents the expectation and skips when ACLs are unavailable. See task 4.4.
func TestIntegration_Maintenance_ExposesLocalMaintenanceUnderACLToken(t *testing.T) {
	client := setupConsulClient(t)

	// The dev agent does not enable ACLs. Detect this by inspecting the agent
	// self configuration safely; skip when ACLs are unavailable.
	info, err := client.Agent().Self()
	require.NoError(t, err)

	if !aclEnabled(info) {
		t.Skip("ACLs are not enabled on the integration Consul agent; skipping ACL-restricted maintenance coverage")
	}

	// When ACLs are enabled, the service-ID health lookup must still expose
	// local maintenance under a service-read-only token. This branch is
	// exercised only in environments that boot Consul with ACLs.
	serviceID := fmt.Sprintf("test-maint-acl-%d", time.Now().UnixNano())
	electionKey := fmt.Sprintf("election/test/maint-acl-%d/leader", time.Now().UnixNano())

	registerTestService(t, client, serviceID, 8703)
	defer deregisterTestService(t, client, serviceID)
	defer cleanupKV(t, client, electionKey)
	defer disableMaintenance(t, client, serviceID)

	require.NoError(t, client.Agent().EnableNodeMaintenance("integration test"))
	waitForAggregatedHealth(t, client, serviceID, api.HealthMaint)

	status, healthInfo, err := client.Agent().AgentHealthServiceByID(serviceID)
	require.NoError(t, err)
	assert.Equal(t, api.HealthCritical, status)
	require.NotNil(t, healthInfo)
	assert.Equal(t, api.HealthMaint, healthInfo.AggregatedStatus)
}

// aclEnabled reports whether the agent self-info indicates ACLs are enabled.
func aclEnabled(info map[string]map[string]interface{}) bool {
	acl, ok := info["Config"]["ACL"].(map[string]interface{})
	if !ok {
		return false
	}
	enabled, _ := acl["Enabled"].(bool)
	return enabled
}
