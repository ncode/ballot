package ballot

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestElectionStep_Run_successfulTransition(t *testing.T) {
	b := stepTestBallot(context.Background())
	sessionID := "session-id"
	payload := &ElectionPayload{Address: "127.0.0.1", Port: 8080, SessionID: sessionID}
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	service := &api.AgentService{ID: b.ID, Service: b.Name, Address: payload.Address, Port: payload.Port, Tags: []string{"blue"}}
	mockAgent := new(MockAgent)
	mockAgent.On("Service", b.ID, mock.Anything).Return(service, nil, nil)
	mockAgent.On("AgentHealthServiceByID", b.ID).Return(api.HealthPassing, &api.AgentServiceChecksInfo{
		AggregatedStatus: api.HealthPassing,
		Service:          service,
		Checks: api.HealthChecks{
			{ServiceID: b.ID, CheckID: "check1", Status: api.HealthPassing},
		},
	}, nil)
	mockAgent.On("ServiceRegister", mock.MatchedBy(func(reg *api.AgentServiceRegistration) bool {
		return assert.ObjectsAreEqual([]string{"blue", "primary"}, reg.Tags)
	})).Return(nil)

	staleService := &api.CatalogService{
		ID:             "catalog-id",
		Node:           "node-1",
		ServiceID:      "stale-service",
		ServiceName:    b.Name,
		ServiceAddress: "127.0.0.2",
		ServicePort:    8081,
		ServiceTags:    []string{"primary", "blue"},
	}
	mockCatalog := new(MockCatalog)
	mockCatalog.On("Service", b.Name, b.PrimaryTag, mock.Anything).Return([]*api.CatalogService{}, nil, nil)
	mockCatalog.On("Service", b.Name, "", (*api.QueryOptions)(nil)).Return([]*api.CatalogService{staleService}, nil, nil)
	mockCatalog.On("Register", mock.MatchedBy(func(reg *api.CatalogRegistration) bool {
		return reg.Service != nil && assert.ObjectsAreEqual([]string{"blue"}, reg.Service.Tags)
	}), (*api.WriteOptions)(nil)).Return(&api.WriteMeta{}, nil)

	mockSession := new(MockSession)
	mockSession.On("Create", mock.Anything, (*api.WriteOptions)(nil)).Return(sessionID, nil, nil)
	mockSession.On("RenewPeriodic", "10s", sessionID, (*api.WriteOptions)(nil), mock.Anything).Return(nil)

	mockKV := new(MockKV)
	mockKV.On("Acquire", mock.Anything, (*api.WriteOptions)(nil)).Return(true, nil, nil)
	mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(&api.KVPair{Key: b.Key, Value: data, Session: sessionID}, nil, nil)

	mockClient := &MockConsulClient{}
	mockClient.On("Agent").Return(mockAgent)
	mockClient.On("Catalog").Return(mockCatalog)
	mockClient.On("Session").Return(mockSession)
	mockClient.On("KV").Return(mockKV)
	b.client = mockClient

	result := NewElectionStep(b).Run()

	require.NoError(t, result.Err)
	assert.Equal(t, ElectionStepLeader, result.Status)
	assert.True(t, result.Leader)
	assert.True(t, b.IsLeader())
	mockAgent.AssertCalled(t, "ServiceRegister", mock.Anything)
	mockCatalog.AssertCalled(t, "Register", mock.Anything, (*api.WriteOptions)(nil))
}

func TestElectionStep_Run_follower(t *testing.T) {
	b := stepTestBallot(context.Background())
	sessionID := "session-id"
	leaderSessionID := "leader-session-id"
	payload := &ElectionPayload{Address: "127.0.0.2", Port: 8081, SessionID: leaderSessionID}
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	mockAgent := new(MockAgent)
	followerService := &api.AgentService{
		ID:      b.ID,
		Service: b.Name,
		Address: "127.0.0.1",
		Port:    8080,
		Tags:    []string{},
	}
	mockAgent.On("Service", b.ID, mock.Anything).Return(followerService, nil, nil)
	mockAgent.On("AgentHealthServiceByID", b.ID).Return(api.HealthPassing, &api.AgentServiceChecksInfo{
		AggregatedStatus: api.HealthPassing,
		Service:          followerService,
		Checks: api.HealthChecks{
			{ServiceID: b.ID, CheckID: "check1", Status: api.HealthPassing},
		},
	}, nil)

	mockCatalog := new(MockCatalog)
	mockCatalog.On("Service", b.Name, b.PrimaryTag, mock.Anything).Return([]*api.CatalogService{}, nil, nil)

	mockSession := new(MockSession)
	mockSession.On("Create", mock.Anything, (*api.WriteOptions)(nil)).Return(sessionID, nil, nil)
	mockSession.On("RenewPeriodic", "10s", sessionID, (*api.WriteOptions)(nil), mock.Anything).Return(nil)

	mockKV := new(MockKV)
	mockKV.On("Acquire", mock.Anything, (*api.WriteOptions)(nil)).Return(false, nil, nil)
	mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(&api.KVPair{Key: b.Key, Value: data, Session: leaderSessionID}, nil, nil)

	mockClient := &MockConsulClient{}
	mockClient.On("Agent").Return(mockAgent)
	mockClient.On("Catalog").Return(mockCatalog)
	mockClient.On("Session").Return(mockSession)
	mockClient.On("KV").Return(mockKV)
	b.client = mockClient

	result := NewElectionStep(b).Run()

	require.NoError(t, result.Err)
	assert.Equal(t, ElectionStepFollower, result.Status)
	assert.False(t, result.Leader)
}

func TestElectionStep_Run_failures(t *testing.T) {
	t.Run("critical health", func(t *testing.T) {
		b := stepTestBallot(context.Background())
		mockAgent := new(MockAgent)
		criticalService := &api.AgentService{ID: b.ID, Service: b.Name, Tags: []string{"primary"}}
		mockAgent.On("Service", b.ID, mock.Anything).Return(criticalService, nil, nil)
		mockAgent.On("AgentHealthServiceByID", b.ID).Return(api.HealthCritical, &api.AgentServiceChecksInfo{
			AggregatedStatus: api.HealthCritical,
			Service:          criticalService,
			Checks: api.HealthChecks{
				{ServiceID: b.ID, CheckID: "check1", Status: api.HealthCritical},
			},
		}, nil)
		mockAgent.On("ServiceRegister", mock.Anything).Return(nil)
		mockCatalog := new(MockCatalog)
		mockCatalog.On("Service", b.Name, b.PrimaryTag, mock.Anything).Return([]*api.CatalogService{}, nil, nil)
		mockClient := &MockConsulClient{}
		mockClient.On("Agent").Return(mockAgent)
		mockClient.On("Catalog").Return(mockCatalog)
		b.client = mockClient

		result := NewElectionStep(b).Run()

		require.Error(t, result.Err)
		assert.Equal(t, ElectionStepCriticalHealth, result.Status)
	})

	t.Run("missing service", func(t *testing.T) {
		b := stepTestBallot(context.Background())
		mockAgent := new(MockAgent)
		mockAgent.On("Service", b.ID, mock.Anything).Return((*api.AgentService)(nil), nil, nil)
		// Consul reports (critical, nil, nil) when no local service matches the
		// configured ID; HealthState treats absence as passing so the local
		// service lookup produces the service-failure result.
		mockAgent.On("AgentHealthServiceByID", b.ID).Return(api.HealthCritical, (*api.AgentServiceChecksInfo)(nil), nil)
		mockClient := &MockConsulClient{}
		mockClient.On("Agent").Return(mockAgent)
		b.client = mockClient

		result := NewElectionStep(b).Run()

		require.Error(t, result.Err)
		assert.Equal(t, ElectionStepServiceFailure, result.Status)
	})

	t.Run("session failure", func(t *testing.T) {
		b := stepTestBallot(context.Background())
		withPassingHealthAndService(b)
		mockSession := new(MockSession)
		mockSession.On("Create", mock.Anything, (*api.WriteOptions)(nil)).Return("", nil, errors.New("create failed"))
		b.client.(*MockConsulClient).On("Session").Return(mockSession)

		result := NewElectionStep(b).Run()

		require.Error(t, result.Err)
		assert.Equal(t, ElectionStepSessionFailure, result.Status)
	})

	t.Run("lock failure", func(t *testing.T) {
		b := stepTestBallot(context.Background())
		withPassingHealthAndService(b)
		sessionID := "session-id"
		mockSession := new(MockSession)
		mockSession.On("Create", mock.Anything, (*api.WriteOptions)(nil)).Return(sessionID, nil, nil)
		mockSession.On("RenewPeriodic", "10s", sessionID, (*api.WriteOptions)(nil), mock.Anything).Return(nil)
		mockKV := new(MockKV)
		mockKV.On("Acquire", mock.Anything, (*api.WriteOptions)(nil)).Return(false, nil, errors.New("lock failed"))
		b.client.(*MockConsulClient).On("Session").Return(mockSession)
		b.client.(*MockConsulClient).On("KV").Return(mockKV)

		result := NewElectionStep(b).Run()

		require.Error(t, result.Err)
		assert.Equal(t, ElectionStepLockFailure, result.Status)
	})

	t.Run("invalid lock payload", func(t *testing.T) {
		b := stepTestBallot(context.Background())
		withPassingHealthAndService(b)
		sessionID := "session-id"
		mockSession := new(MockSession)
		mockSession.On("Create", mock.Anything, (*api.WriteOptions)(nil)).Return(sessionID, nil, nil)
		mockSession.On("RenewPeriodic", "10s", sessionID, (*api.WriteOptions)(nil), mock.Anything).Return(nil)
		mockKV := new(MockKV)
		mockKV.On("Acquire", mock.Anything, (*api.WriteOptions)(nil)).Return(true, nil, nil)
		mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(&api.KVPair{Key: b.Key, Value: []byte("{")}, nil, nil)
		b.client.(*MockConsulClient).On("Session").Return(mockSession)
		b.client.(*MockConsulClient).On("KV").Return(mockKV)

		result := NewElectionStep(b).Run()

		require.Error(t, result.Err)
		assert.Equal(t, ElectionStepPayloadFailure, result.Status)
	})

	t.Run("cleanup failure", func(t *testing.T) {
		b := stepTestBallot(context.Background())
		sessionID := "session-id"
		payload := &ElectionPayload{Address: "127.0.0.1", Port: 8080, SessionID: sessionID}
		data, err := json.Marshal(payload)
		require.NoError(t, err)
		mockAgent := new(MockAgent)
		cleanupService := &api.AgentService{
			ID:      b.ID,
			Service: b.Name,
			Address: "127.0.0.1",
			Port:    8080,
			Tags:    []string{},
		}
		mockAgent.On("Service", b.ID, mock.Anything).Return(cleanupService, nil, nil)
		mockAgent.On("AgentHealthServiceByID", b.ID).Return(api.HealthPassing, &api.AgentServiceChecksInfo{
			AggregatedStatus: api.HealthPassing,
			Service:          cleanupService,
			Checks: api.HealthChecks{
				{ServiceID: b.ID, CheckID: "check1", Status: api.HealthPassing},
			},
		}, nil)
		mockAgent.On("ServiceRegister", mock.Anything).Return(nil)
		mockSession := new(MockSession)
		mockSession.On("Create", mock.Anything, (*api.WriteOptions)(nil)).Return(sessionID, nil, nil)
		mockSession.On("RenewPeriodic", "10s", sessionID, (*api.WriteOptions)(nil), mock.Anything).Return(nil)
		mockKV := new(MockKV)
		mockKV.On("Acquire", mock.Anything, (*api.WriteOptions)(nil)).Return(true, nil, nil)
		mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(&api.KVPair{Key: b.Key, Value: data}, nil, nil)
		mockClient := &MockConsulClient{}
		mockClient.On("Agent").Return(mockAgent)
		mockClient.On("Catalog").Return(&cleanupFailingCatalog{MockCatalog: new(MockCatalog), serviceName: b.Name})
		mockClient.On("Session").Return(mockSession)
		mockClient.On("KV").Return(mockKV)
		b.client = mockClient

		result := NewElectionStep(b).Run()

		require.Error(t, result.Err)
		assert.Equal(t, ElectionStepCleanupFailure, result.Status)
	})
}

func stepTestBallot(ctx context.Context) *Ballot {
	return &Ballot{
		ID:            "test_service_id",
		Name:          "test_service",
		Key:           "election/test_service/leader",
		PrimaryTag:    "primary",
		ServiceChecks: []string{"check1"},
		TTL:           10 * time.Second,
		LockDelay:     3 * time.Second,
		ctx:           ctx,
	}
}

func withPassingHealthAndService(b *Ballot) {
	mockAgent := new(MockAgent)
	service := &api.AgentService{
		ID:      b.ID,
		Service: b.Name,
		Address: "127.0.0.1",
		Port:    8080,
		Tags:    []string{},
	}
	mockAgent.On("Service", b.ID, mock.Anything).Return(service, nil, nil)
	mockAgent.On("AgentHealthServiceByID", b.ID).Return(api.HealthPassing, &api.AgentServiceChecksInfo{
		AggregatedStatus: api.HealthPassing,
		Service:          service,
		Checks: api.HealthChecks{
			{ServiceID: b.ID, CheckID: "check1", Status: api.HealthPassing},
		},
	}, nil)
	mockAgent.On("ServiceRegister", mock.Anything).Return(nil)
	mockCatalog := new(MockCatalog)
	mockCatalog.On("Service", b.Name, b.PrimaryTag, mock.Anything).Return([]*api.CatalogService{}, nil, nil)
	mockCatalog.On("Service", b.Name, "", (*api.QueryOptions)(nil)).Return([]*api.CatalogService{}, nil, nil)
	mockClient := &MockConsulClient{}
	mockClient.On("Agent").Return(mockAgent)
	mockClient.On("Catalog").Return(mockCatalog)
	b.client = mockClient
}

type cleanupFailingCatalog struct {
	*MockCatalog
	serviceName string
}

func (c *cleanupFailingCatalog) Service(serviceName, tag string, q *api.QueryOptions) ([]*api.CatalogService, *api.QueryMeta, error) {
	if serviceName == c.serviceName && tag == "" {
		return nil, nil, errors.New("cleanup failed")
	}
	return []*api.CatalogService{}, nil, nil
}

// maintenanceHealthInfo builds a local Agent health response reporting the
// given maintenance check id alongside an otherwise passing configured check.
func maintenanceHealthInfo(serviceID, serviceName, maintenanceCheckID string) *api.AgentServiceChecksInfo {
	return &api.AgentServiceChecksInfo{
		AggregatedStatus: api.HealthMaint,
		Service:          &api.AgentService{ID: serviceID, Service: serviceName},
		Checks: api.HealthChecks{
			{CheckID: maintenanceCheckID, Status: api.HealthCritical},
			{ServiceID: serviceID, CheckID: "check1", Status: api.HealthPassing},
		},
	}
}

// TestElectionStep_Run_maintainedFollower proves that a follower under local
// node or service maintenance does not create a session, attempt KV
// acquisition, gain leadership, or receive the primary tag. See task 3.1.
func TestElectionStep_Run_maintainedFollower(t *testing.T) {
	cases := []struct {
		name             string
		maintenanceCheck string
	}{
		{"node maintenance", api.NodeMaint},
		{"service maintenance", api.ServiceMaintPrefix + "test_service_id"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := stepTestBallot(context.Background())

			mockAgent := new(MockAgent)
			mockAgent.On("AgentHealthServiceByID", b.ID).Return(
				api.HealthCritical,
				maintenanceHealthInfo(b.ID, b.Name, tc.maintenanceCheck),
				nil,
			)
			// Relinquishment clears leadership and removes the primary tag; the
			// follower has none, so no ServiceRegister should occur.
			mockAgent.On("Service", b.ID, mock.Anything).Return(&api.AgentService{
				ID:      b.ID,
				Service: b.Name,
				Tags:    []string{},
			}, nil, nil)
			mockCatalog := new(MockCatalog)
			mockCatalog.On("Service", b.Name, b.PrimaryTag, mock.Anything).Return([]*api.CatalogService{}, nil, nil)

			mockClient := &MockConsulClient{}
			mockClient.On("Agent").Return(mockAgent)
			mockClient.On("Catalog").Return(mockCatalog)
			b.client = mockClient

			result := NewElectionStep(b).Run()

			require.Error(t, result.Err)
			assert.Equal(t, ElectionStepCriticalHealth, result.Status)
			assert.False(t, result.Leader)
			assert.False(t, b.IsLeader())
			// A maintained follower must not create a session or acquire the lock.
			mockAgent.AssertNotCalled(t, "ServiceRegister")
		})
	}
}

// TestElectionStep_Run_maintainedLeader proves that a leader under local node
// or service maintenance releases its session, becomes non-leader, removes the
// primary tag, and returns the existing critical-health result. It also covers
// the fail-closed scenario where session destruction fails. See task 3.2.
func TestElectionStep_Run_maintainedLeader(t *testing.T) {
	cases := []struct {
		name             string
		maintenanceCheck string
	}{
		{"node maintenance", api.NodeMaint},
		{"service maintenance", api.ServiceMaintPrefix + "test_service_id"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := stepTestBallot(context.Background())
			sessionID := "leader-session"

			mockAgent := new(MockAgent)
			mockAgent.On("AgentHealthServiceByID", b.ID).Return(
				api.HealthCritical,
				maintenanceHealthInfo(b.ID, b.Name, tc.maintenanceCheck),
				nil,
			)
			// The leader currently advertises the primary tag; relinquishment
			// must remove it.
			mockAgent.On("Service", b.ID, mock.Anything).Return(&api.AgentService{
				ID:      b.ID,
				Service: b.Name,
				Tags:    []string{"primary"},
			}, nil, nil)
			mockAgent.On("ServiceRegister", mock.MatchedBy(func(reg *api.AgentServiceRegistration) bool {
				return !containsString(reg.Tags, b.PrimaryTag)
			})).Return(nil)
			mockCatalog := new(MockCatalog)
			mockCatalog.On("Service", b.Name, b.PrimaryTag, mock.Anything).Return([]*api.CatalogService{}, nil, nil)

			mockSession := new(MockSession)
			mockSession.On("Destroy", sessionID, (*api.WriteOptions)(nil)).Return(nil, nil)

			mockClient := &MockConsulClient{}
			mockClient.On("Agent").Return(mockAgent)
			mockClient.On("Catalog").Return(mockCatalog)
			mockClient.On("Session").Return(mockSession)
			b.client = mockClient

			b.sessionID.Store(&sessionID)
			b.leader.Store(true)

			result := NewElectionStep(b).Run()

			require.Error(t, result.Err)
			assert.Equal(t, ElectionStepCriticalHealth, result.Status)
			assert.False(t, result.Leader)
			assert.False(t, b.IsLeader())
			mockSession.AssertCalled(t, "Destroy", sessionID, (*api.WriteOptions)(nil))
			mockAgent.AssertCalled(t, "ServiceRegister", mock.Anything)
		})

		t.Run(tc.name+" fail-closed on session destroy error", func(t *testing.T) {
			b := stepTestBallot(context.Background())
			sessionID := "leader-session"

			mockAgent := new(MockAgent)
			mockAgent.On("AgentHealthServiceByID", b.ID).Return(
				api.HealthCritical,
				maintenanceHealthInfo(b.ID, b.Name, tc.maintenanceCheck),
				nil,
			)
			mockAgent.On("Service", b.ID, mock.Anything).Return(&api.AgentService{
				ID:      b.ID,
				Service: b.Name,
				Tags:    []string{"primary"},
			}, nil, nil)
			mockAgent.On("ServiceRegister", mock.MatchedBy(func(reg *api.AgentServiceRegistration) bool {
				return !containsString(reg.Tags, b.PrimaryTag)
			})).Return(nil)
			mockCatalog := new(MockCatalog)
			mockCatalog.On("Service", b.Name, b.PrimaryTag, mock.Anything).Return([]*api.CatalogService{}, nil, nil)

			mockSession := new(MockSession)
			mockSession.On("Destroy", sessionID, (*api.WriteOptions)(nil)).Return(nil, errors.New("destroy failed"))

			mockClient := &MockConsulClient{}
			mockClient.On("Agent").Return(mockAgent)
			mockClient.On("Catalog").Return(mockCatalog)
			mockClient.On("Session").Return(mockSession)
			b.client = mockClient

			b.sessionID.Store(&sessionID)
			b.leader.Store(true)

			result := NewElectionStep(b).Run()

			require.Error(t, result.Err)
			assert.Contains(t, result.Err.Error(), "failed to release session")
			assert.Equal(t, ElectionStepCriticalHealth, result.Status)
			// Fail-closed: local leadership is cleared and primary tag removal is
			// attempted even though session destruction failed.
			assert.False(t, b.leader.Load())
			mockAgent.AssertCalled(t, "ServiceRegister", mock.Anything)
		})
	}
}

// TestElectionStep_Run_maintenanceRecovery proves that an instance can
// participate on the next election step after maintenance is disabled and the
// configured checks are eligible. See task 3.5.
func TestElectionStep_Run_maintenanceRecovery(t *testing.T) {
	b := stepTestBallot(context.Background())
	sessionID := "session-id"
	payload := &ElectionPayload{Address: "127.0.0.1", Port: 8080, SessionID: sessionID}
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	mockAgent := new(MockAgent)
	service := &api.AgentService{ID: b.ID, Service: b.Name, Address: payload.Address, Port: payload.Port, Tags: []string{}}

	// First election step: node maintenance is active.
	mockAgent.On("AgentHealthServiceByID", b.ID).Return(
		api.HealthCritical,
		maintenanceHealthInfo(b.ID, b.Name, api.NodeMaint),
		nil,
	).Once()
	// Second election step: maintenance disabled, configured check passing.
	mockAgent.On("AgentHealthServiceByID", b.ID).Return(api.HealthPassing, &api.AgentServiceChecksInfo{
		AggregatedStatus: api.HealthPassing,
		Service:          service,
		Checks: api.HealthChecks{
			{ServiceID: b.ID, CheckID: "check1", Status: api.HealthPassing},
		},
	}, nil).Once()
	mockAgent.On("Service", b.ID, mock.Anything).Return(service, nil, nil)
	mockAgent.On("ServiceRegister", mock.MatchedBy(func(reg *api.AgentServiceRegistration) bool {
		return containsString(reg.Tags, b.PrimaryTag)
	})).Return(nil)

	mockCatalog := new(MockCatalog)
	mockCatalog.On("Service", b.Name, b.PrimaryTag, mock.Anything).Return([]*api.CatalogService{}, nil, nil)
	mockCatalog.On("Service", b.Name, "", (*api.QueryOptions)(nil)).Return([]*api.CatalogService{}, nil, nil)

	mockSession := new(MockSession)
	mockSession.On("Create", mock.Anything, (*api.WriteOptions)(nil)).Return(sessionID, nil, nil)
	mockSession.On("RenewPeriodic", "10s", sessionID, (*api.WriteOptions)(nil), mock.Anything).Return(nil)

	mockKV := new(MockKV)
	mockKV.On("Acquire", mock.Anything, (*api.WriteOptions)(nil)).Return(true, nil, nil)
	mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(&api.KVPair{Key: b.Key, Value: data, Session: sessionID}, nil, nil)

	mockClient := &MockConsulClient{}
	mockClient.On("Agent").Return(mockAgent)
	mockClient.On("Catalog").Return(mockCatalog)
	mockClient.On("Session").Return(mockSession)
	mockClient.On("KV").Return(mockKV)
	b.client = mockClient

	// Step 1: maintenance blocks election.
	first := NewElectionStep(b).Run()
	require.Error(t, first.Err)
	assert.Equal(t, ElectionStepCriticalHealth, first.Status)
	assert.False(t, b.IsLeader())

	// Step 2: maintenance disabled; instance can be elected.
	second := NewElectionStep(b).Run()
	require.NoError(t, second.Err)
	assert.Equal(t, ElectionStepLeader, second.Status)
	assert.True(t, b.IsLeader())
}

// containsString is a small test helper used by matcher functions above.
func containsString(slice []string, target string) bool {
	for _, s := range slice {
		if s == target {
			return true
		}
	}
	return false
}
