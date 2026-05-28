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

	mockHealth := new(MockHealth)
	mockHealth.On("Checks", b.Name, (*api.QueryOptions)(nil)).Return(api.HealthChecks{
		{ServiceID: b.ID, CheckID: "check1", Status: "passing"},
	}, nil, nil)

	service := &api.AgentService{ID: b.ID, Service: b.Name, Address: payload.Address, Port: payload.Port, Tags: []string{"blue"}}
	mockAgent := new(MockAgent)
	mockAgent.On("Service", b.ID, mock.Anything).Return(service, nil, nil)
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
	mockClient.On("Health").Return(mockHealth)
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

	mockHealth := new(MockHealth)
	mockHealth.On("Checks", b.Name, (*api.QueryOptions)(nil)).Return(api.HealthChecks{
		{ServiceID: b.ID, CheckID: "check1", Status: "passing"},
	}, nil, nil)

	mockAgent := new(MockAgent)
	mockAgent.On("Service", b.ID, mock.Anything).Return(&api.AgentService{
		ID:      b.ID,
		Service: b.Name,
		Address: "127.0.0.1",
		Port:    8080,
		Tags:    []string{},
	}, nil, nil)

	mockCatalog := new(MockCatalog)
	mockCatalog.On("Service", b.Name, b.PrimaryTag, mock.Anything).Return([]*api.CatalogService{}, nil, nil)

	mockSession := new(MockSession)
	mockSession.On("Create", mock.Anything, (*api.WriteOptions)(nil)).Return(sessionID, nil, nil)
	mockSession.On("RenewPeriodic", "10s", sessionID, (*api.WriteOptions)(nil), mock.Anything).Return(nil)

	mockKV := new(MockKV)
	mockKV.On("Acquire", mock.Anything, (*api.WriteOptions)(nil)).Return(false, nil, nil)
	mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(&api.KVPair{Key: b.Key, Value: data, Session: leaderSessionID}, nil, nil)

	mockClient := &MockConsulClient{}
	mockClient.On("Health").Return(mockHealth)
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
		mockHealth := new(MockHealth)
		mockHealth.On("Checks", b.Name, (*api.QueryOptions)(nil)).Return(api.HealthChecks{
			{ServiceID: b.ID, CheckID: "check1", Status: "critical"},
		}, nil, nil)
		mockAgent := new(MockAgent)
		mockAgent.On("Service", b.ID, mock.Anything).Return(&api.AgentService{ID: b.ID, Service: b.Name, Tags: []string{"primary"}}, nil, nil)
		mockAgent.On("ServiceRegister", mock.Anything).Return(nil)
		mockCatalog := new(MockCatalog)
		mockCatalog.On("Service", b.Name, b.PrimaryTag, mock.Anything).Return([]*api.CatalogService{}, nil, nil)
		mockClient := &MockConsulClient{}
		mockClient.On("Health").Return(mockHealth)
		mockClient.On("Agent").Return(mockAgent)
		mockClient.On("Catalog").Return(mockCatalog)
		b.client = mockClient

		result := NewElectionStep(b).Run()

		require.Error(t, result.Err)
		assert.Equal(t, ElectionStepCriticalHealth, result.Status)
	})

	t.Run("missing service", func(t *testing.T) {
		b := stepTestBallot(context.Background())
		mockHealth := new(MockHealth)
		mockHealth.On("Checks", b.Name, (*api.QueryOptions)(nil)).Return(api.HealthChecks{
			{ServiceID: b.ID, CheckID: "check1", Status: "passing"},
		}, nil, nil)
		mockAgent := new(MockAgent)
		mockAgent.On("Service", b.ID, mock.Anything).Return((*api.AgentService)(nil), nil, nil)
		mockClient := &MockConsulClient{}
		mockClient.On("Health").Return(mockHealth)
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
		mockHealth := new(MockHealth)
		mockHealth.On("Checks", b.Name, (*api.QueryOptions)(nil)).Return(api.HealthChecks{
			{ServiceID: b.ID, CheckID: "check1", Status: "passing"},
		}, nil, nil)
		mockAgent := new(MockAgent)
		mockAgent.On("Service", b.ID, mock.Anything).Return(&api.AgentService{
			ID:      b.ID,
			Service: b.Name,
			Address: "127.0.0.1",
			Port:    8080,
			Tags:    []string{},
		}, nil, nil)
		mockAgent.On("ServiceRegister", mock.Anything).Return(nil)
		mockSession := new(MockSession)
		mockSession.On("Create", mock.Anything, (*api.WriteOptions)(nil)).Return(sessionID, nil, nil)
		mockSession.On("RenewPeriodic", "10s", sessionID, (*api.WriteOptions)(nil), mock.Anything).Return(nil)
		mockKV := new(MockKV)
		mockKV.On("Acquire", mock.Anything, (*api.WriteOptions)(nil)).Return(true, nil, nil)
		mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(&api.KVPair{Key: b.Key, Value: data}, nil, nil)
		mockClient := &MockConsulClient{}
		mockClient.On("Health").Return(mockHealth)
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
	mockHealth := new(MockHealth)
	mockHealth.On("Checks", b.Name, (*api.QueryOptions)(nil)).Return(api.HealthChecks{
		{ServiceID: b.ID, CheckID: "check1", Status: "passing"},
	}, nil, nil)
	mockAgent := new(MockAgent)
	mockAgent.On("Service", b.ID, mock.Anything).Return(&api.AgentService{
		ID:      b.ID,
		Service: b.Name,
		Address: "127.0.0.1",
		Port:    8080,
		Tags:    []string{},
	}, nil, nil)
	mockAgent.On("ServiceRegister", mock.Anything).Return(nil)
	mockCatalog := new(MockCatalog)
	mockCatalog.On("Service", b.Name, b.PrimaryTag, mock.Anything).Return([]*api.CatalogService{}, nil, nil)
	mockCatalog.On("Service", b.Name, "", (*api.QueryOptions)(nil)).Return([]*api.CatalogService{}, nil, nil)
	mockClient := &MockConsulClient{}
	mockClient.On("Health").Return(mockHealth)
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
