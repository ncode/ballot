//go:build integration

package ballot

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/spf13/viper"
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

func setupViper(t *testing.T, serviceID string, electionKey string) {
	t.Helper()
	viper.Reset()
	viper.Set("consul.address", getConsulAddr())
	viper.Set("consul.token", "")
	viper.Set(fmt.Sprintf("election.services.%s.id", serviceID), serviceID)
	viper.Set(fmt.Sprintf("election.services.%s.key", serviceID), electionKey)
	viper.Set(fmt.Sprintf("election.services.%s.primaryTag", serviceID), testPrimaryTag)
	viper.Set(fmt.Sprintf("election.services.%s.serviceChecks", serviceID), []string{fmt.Sprintf("service:%s", serviceID)})
	viper.Set(fmt.Sprintf("election.services.%s.ttl", serviceID), "10s")
	viper.Set(fmt.Sprintf("election.services.%s.lockDelay", serviceID), "1s")
}

func TestIntegration_FullElectionCycle(t *testing.T) {
	client := setupConsulClient(t)

	serviceID := fmt.Sprintf("test-service-%d", time.Now().UnixNano())
	electionKey := fmt.Sprintf("election/test/%s/leader", serviceID)

	// Register test service
	registerTestService(t, client, serviceID, 8080)
	defer deregisterTestService(t, client, serviceID)
	defer cleanupKV(t, client, electionKey)

	// Setup viper configuration
	setupViper(t, serviceID, electionKey)
	defer viper.Reset()

	// Create ballot instance
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ballot, err := New(ctx, serviceID)
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

	// Setup and create first ballot
	// Note: Ballot copies config at creation time, so resetting viper after
	// ballot1 is created won't affect it.
	setupViper(t, serviceID1, electionKey)
	ballot1, err := New(ctx, serviceID1)
	require.NoError(t, err)
	defer ballot1.releaseSession()

	// Setup and create second ballot with fresh viper config
	setupViper(t, serviceID2, electionKey)
	ballot2, err := New(ctx, serviceID2)
	require.NoError(t, err)
	defer ballot2.releaseSession()
	defer viper.Reset()

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

	// Wait for lock delay to pass
	time.Sleep(2 * time.Second)

	// Second ballot should now be able to become leader
	err = ballot2.election()
	require.NoError(t, err)
	assert.True(t, ballot2.IsLeader(), "Ballot 2 should be leader after failover")

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

	setupViper(t, serviceID, electionKey)
	defer viper.Reset()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ballot, err := New(ctx, serviceID)
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

	setupViper(t, serviceID, electionKey)
	defer viper.Reset()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ballot, err := New(ctx, serviceID)
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

		setupViper(t, services[i], electionKey)
		b, err := New(ctx, services[i])
		require.NoError(t, err)
		defer b.releaseSession()
		ballots[i] = b
	}
	defer cleanupKV(t, client, electionKey)
	defer viper.Reset()

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

	// Use short TTL to test renewal
	viper.Reset()
	viper.Set("consul.address", getConsulAddr())
	viper.Set(fmt.Sprintf("election.services.%s.id", serviceID), serviceID)
	viper.Set(fmt.Sprintf("election.services.%s.key", serviceID), electionKey)
	viper.Set(fmt.Sprintf("election.services.%s.primaryTag", serviceID), testPrimaryTag)
	viper.Set(fmt.Sprintf("election.services.%s.serviceChecks", serviceID), []string{fmt.Sprintf("service:%s", serviceID)})
	viper.Set(fmt.Sprintf("election.services.%s.ttl", serviceID), "10s")
	viper.Set(fmt.Sprintf("election.services.%s.lockDelay", serviceID), "1s")
	defer viper.Reset()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ballot, err := New(ctx, serviceID)
	require.NoError(t, err)
	defer ballot.releaseSession()

	// Become leader
	err = ballot.election()
	require.NoError(t, err)
	assert.True(t, ballot.IsLeader())

	sessionID, ok := ballot.getSessionID()
	require.True(t, ok)
	require.NotNil(t, sessionID)

	// Wait longer than TTL/2 but less than TTL to verify session is still valid
	time.Sleep(6 * time.Second)

	// Session should still be valid due to renewal
	sessionInfo, _, err := client.Session().Info(*sessionID, nil)
	require.NoError(t, err)
	assert.NotNil(t, sessionInfo, "Session should still exist after renewal")

	// Run another election - should maintain leadership
	err = ballot.election()
	require.NoError(t, err)
	assert.True(t, ballot.IsLeader(), "Should still be leader after session renewal")
}
