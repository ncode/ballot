package ballot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"slices"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNew(t *testing.T) {
	t.Run("successful new from runtime config", func(t *testing.T) {
		b, err := New(context.Background(), RuntimeConfig{
			Name:          "test",
			ID:            "test_service_id",
			Key:           "election/test_service/leader",
			PrimaryTag:    "primary",
			ServiceChecks: []string{"service:test_service_id"},
			ExecOnPromote: "echo Promoted to leader",
			ExecOnDemote:  "echo Demoted from leader",
			TTL:           10 * time.Second,
			LockDelay:     3 * time.Second,
		})
		assert.NoError(t, err)
		assert.NotNil(t, b)
		assert.Equal(t, "test_service_id", b.ID)
		assert.Equal(t, "election/test_service/leader", b.Key)
		assert.Equal(t, "primary", b.PrimaryTag)
	})

	t.Run("failure due to nil context", func(t *testing.T) {
		b, err := New(nil, RuntimeConfig{
			Name:      "test",
			ID:        "test_service_id",
			Key:       "election/test_service/leader",
			TTL:       10 * time.Second,
			LockDelay: 3 * time.Second,
		})
		assert.Error(t, err)
		assert.Nil(t, b)
	})

	t.Run("failure due to missing key", func(t *testing.T) {
		b, err := New(context.Background(), RuntimeConfig{
			Name:      "nokey",
			ID:        "test_service_id",
			TTL:       10 * time.Second,
			LockDelay: 3 * time.Second,
		})
		assert.Error(t, err)
		assert.Nil(t, b)
		assert.Contains(t, err.Error(), "key is required")
	})

	t.Run("failure due to missing id", func(t *testing.T) {
		b, err := New(context.Background(), RuntimeConfig{
			Name:      "noid",
			Key:       "election/test/leader",
			TTL:       10 * time.Second,
			LockDelay: 3 * time.Second,
		})
		assert.Error(t, err)
		assert.Nil(t, b)
		assert.Contains(t, err.Error(), "service ID is required")
	})

	t.Run("failure due to invalid consul address scheme", func(t *testing.T) {
		b, err := New(context.Background(), RuntimeConfig{
			Name:          "test",
			ID:            "test_service_id",
			Key:           "election/test_service/leader",
			ConsulAddress: "gopher://127.0.0.1:8500",
			TTL:           10 * time.Second,
			LockDelay:     3 * time.Second,
		})

		assert.Error(t, err)
		assert.Nil(t, b)
		assert.Contains(t, err.Error(), "Unknown protocol scheme")
	})
}

func TestConsulClientAccessors(t *testing.T) {
	apiClient, err := api.NewClient(api.DefaultConfig())
	assert.NoError(t, err)

	client := &consulClient{client: apiClient}

	assert.NotNil(t, client.Agent())
	assert.NotNil(t, client.Catalog())
	assert.NotNil(t, client.Health())
	assert.NotNil(t, client.Session())
	assert.NotNil(t, client.KV())
}

func TestCopyServiceToRegistration(t *testing.T) {
	t.Run("successful copy", func(t *testing.T) {
		service := &api.AgentService{
			ID:      "testID",
			Service: "testService",
			Tags:    []string{"tag1", "tag2"},
			Port:    8080,
			Address: "127.0.0.1",
		}

		registration := copyServiceToRegistration(service)

		assert.Equal(t, service.ID, registration.ID)
		assert.Equal(t, service.Service, registration.Name)
		assert.Equal(t, service.Tags, registration.Tags)
		assert.Equal(t, service.Port, registration.Port)
		assert.Equal(t, service.Address, registration.Address)
	})

	t.Run("handles nil service gracefully", func(t *testing.T) {
		registration := copyServiceToRegistration(nil)
		assert.Nil(t, registration)
	})
}

func TestCopyCatalogServiceToRegistration(t *testing.T) {
	t.Run("successful copy", func(t *testing.T) {
		service := &api.CatalogService{
			ID:                       "id",
			Node:                     "node",
			ServiceAddress:           "127.0.0.1",
			ServiceID:                "serviceId",
			ServiceName:              "serviceName",
			ServicePort:              8080,
			ServiceTags:              []string{"tag1", "tag2"},
			ServiceMeta:              map[string]string{"key": "value"},
			ServiceWeights:           api.Weights{Passing: 1, Warning: 1},
			ServiceEnableTagOverride: true,
		}

		registration := copyCatalogServiceToRegistration(service)
		assert.Equal(t, service.ID, registration.ID)
		assert.Equal(t, service.Node, registration.Node)
		assert.Equal(t, service.ServiceAddress, registration.Address)
		assert.Equal(t, service.ServiceID, registration.Service.ID)
		assert.Equal(t, service.ServiceName, registration.Service.Service)
		assert.Equal(t, service.ServicePort, registration.Service.Port)
		assert.Equal(t, service.ServiceTags, registration.Service.Tags)
		assert.Equal(t, service.ServiceMeta, registration.Service.Meta)
		assert.Equal(t, service.ServiceWeights.Passing, registration.Service.Weights.Passing)
		assert.Equal(t, service.ServiceWeights.Warning, registration.Service.Weights.Warning)
		assert.Equal(t, service.ServiceEnableTagOverride, registration.Service.EnableTagOverride)
	})

	t.Run("handles nil service gracefully", func(t *testing.T) {
		registration := copyCatalogServiceToRegistration(nil)
		assert.Nil(t, registration)
	})
}

// MockCommandExecutor is a mock implementation of the CommandExecutor interface
type MockCommandExecutor struct {
	mock.Mock
}

func (m *MockCommandExecutor) CommandContext(ctx context.Context, name string, arg ...string) *exec.Cmd {
	args := m.Called(ctx, name, arg)
	return args.Get(0).(*exec.Cmd)
}

func TestIsLeader(t *testing.T) {
	t.Run("returns true when the ballot is the leader", func(t *testing.T) {
		b := &Ballot{}
		b.leader.Store(true)
		sessionID := "session"
		b.sessionID.Store(&sessionID)
		assert.True(t, b.IsLeader())
	})

	t.Run("returns false when the ballot is not the leader", func(t *testing.T) {
		b := &Ballot{}
		b.leader.Store(false)
		sessionID := "session"
		b.sessionID.Store(&sessionID)
		assert.False(t, b.IsLeader())
	})

	t.Run("returns false when the sessionID is nil", func(t *testing.T) {
		b := &Ballot{}
		b.leader.Store(true)
		b.sessionID.Store((*string)(nil))
		assert.False(t, b.IsLeader())
	})

	t.Run("returns false when the ballot hasn't stored a state yet", func(t *testing.T) {
		b := &Ballot{}
		assert.False(t, b.IsLeader())
	})
}

func TestGetService(t *testing.T) {
	t.Run("service is found successfully", func(t *testing.T) {
		// Set up the mock Agent
		mockAgent := new(MockAgent)
		serviceID := "test_service_id"
		serviceName := "test_service"

		mockAgent.On("Service", serviceID, mock.Anything).Return(&api.AgentService{
			ID:      serviceID,
			Service: serviceName,
		}, nil, nil)

		// Set up the mock Catalog
		mockCatalog := new(MockCatalog)
		mockCatalog.On("Service", serviceName, "primary", mock.Anything).Return([]*api.CatalogService{
			{
				ServiceID:   serviceID,
				ServiceName: serviceName,
			},
		}, nil, nil)

		// Set up the mock ConsulClient
		mockClient := &MockConsulClient{}
		mockClient.On("Agent").Return(mockAgent)
		mockClient.On("Catalog").Return(mockCatalog)
		mockClient.On("Health").Return(new(MockHealth))
		mockClient.On("Session").Return(new(MockSession))
		mockClient.On("KV").Return(new(MockKV))

		b := &Ballot{
			client:     mockClient,
			ID:         serviceID,
			Name:       serviceName,
			PrimaryTag: "primary",
		}

		service, catalogServices, err := b.getService()
		assert.NoError(t, err)
		assert.NotNil(t, service)
		assert.NotNil(t, catalogServices)
		assert.Equal(t, serviceID, service.ID)
		assert.Equal(t, serviceName, service.Service)
		assert.Equal(t, 1, len(catalogServices))
		assert.Equal(t, serviceID, catalogServices[0].ServiceID)
	})
}

func TestIsLeader_EdgeCases(t *testing.T) {
	t.Run("leader is true but sessionID is nil", func(t *testing.T) {
		b := &Ballot{}
		b.leader.Store(true)
		b.sessionID.Store((*string)(nil))
		assert.False(t, b.IsLeader())
	})

	t.Run("leader status changes dynamically", func(t *testing.T) {
		b := &Ballot{}
		sessionID := "session"
		b.sessionID.Store(&sessionID)

		b.leader.Store(false)
		assert.False(t, b.IsLeader())

		b.leader.Store(true)
		assert.True(t, b.IsLeader())

		b.leader.Store(false)
		assert.False(t, b.IsLeader())
	})
}

func TestReleaseSession(t *testing.T) {
	t.Run("session ID is nil", func(t *testing.T) {
		b := &Ballot{
			client: &MockConsulClient{},
		}
		err := b.releaseSession()
		assert.NoError(t, err)
	})

	t.Run("session is successfully destroyed", func(t *testing.T) {
		sessionID := "session"
		b := &Ballot{}
		b.sessionID.Store(&sessionID)

		mockSession := new(MockSession)
		mockSession.On("Destroy", sessionID, (*api.WriteOptions)(nil)).Return(nil, nil)

		mockClient := &MockConsulClient{}
		mockClient.On("Session").Return(mockSession)

		b.client = mockClient

		err := b.releaseSession()
		assert.NoError(t, err)
		sessionIDPtr, ok := b.getSessionID()
		assert.True(t, ok)          // Expect ok to be true
		assert.Nil(t, sessionIDPtr) // sessionIDPtr should be nil
	})

	t.Run("error occurs when destroying session", func(t *testing.T) {
		sessionID := "session"
		b := &Ballot{}
		b.sessionID.Store(&sessionID)

		expectedErr := fmt.Errorf("failed to destroy session")
		mockSession := new(MockSession)
		mockSession.On("Destroy", sessionID, (*api.WriteOptions)(nil)).Return(nil, expectedErr)

		mockClient := &MockConsulClient{}
		mockClient.On("Session").Return(mockSession)

		b.client = mockClient

		err := b.releaseSession()
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})
}

func TestGetSessionID(t *testing.T) {
	b := &Ballot{}

	t.Run("session ID is set", func(t *testing.T) {
		sessionID := "session"
		b.sessionID.Store(&sessionID)
		id, ok := b.getSessionID()
		assert.True(t, ok)
		assert.NotNil(t, id)
		assert.Equal(t, sessionID, *id)
	})

	t.Run("session ID is nil", func(t *testing.T) {
		b.sessionID.Store((*string)(nil))
		id, ok := b.getSessionID()
		assert.True(t, ok)
		assert.Nil(t, id)
	})
}

func TestSession(t *testing.T) {
	t.Run("session is created successfully", func(t *testing.T) {
		sessionID := "session"

		mockSession := new(MockSession)
		mockSession.On("Create", mock.Anything, (*api.WriteOptions)(nil)).Return(sessionID, nil, nil)
		mockSession.On("RenewPeriodic", mock.Anything, sessionID, (*api.WriteOptions)(nil), mock.Anything).Return(nil)

		mockClient := &MockConsulClient{}
		mockClient.On("Session").Return(mockSession)

		b := &Ballot{
			client: mockClient,
			TTL:    10 * time.Second,
			ctx:    context.Background(),
		}

		err := b.session()
		assert.NoError(t, err)
		storedSessionID, ok := b.getSessionID()
		assert.True(t, ok)
		assert.NotNil(t, storedSessionID)
		assert.Equal(t, sessionID, *storedSessionID)
	})

	t.Run("session creation fails", func(t *testing.T) {
		expectedErr := fmt.Errorf("session creation error")

		mockSession := new(MockSession)
		mockSession.On("Create", mock.Anything, (*api.WriteOptions)(nil)).Return("", nil, expectedErr)

		mockClient := &MockConsulClient{}
		mockClient.On("Session").Return(mockSession)

		b := &Ballot{
			client: mockClient,
			TTL:    10 * time.Second,
			ctx:    context.Background(),
		}

		err := b.session()
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})

	t.Run("session cancels previous renewal when creating new session", func(t *testing.T) {
		firstSessionID := "session1"
		secondSessionID := "session2"

		mockSession := new(MockSession)
		// First session creation
		mockSession.On("Create", mock.Anything, (*api.WriteOptions)(nil)).Return(firstSessionID, nil, nil).Once()
		mockSession.On("RenewPeriodic", mock.Anything, firstSessionID, (*api.WriteOptions)(nil), mock.Anything).Return(nil)
		// Info check for first session (returns nil to indicate session expired, forcing new session creation)
		mockSession.On("Info", firstSessionID, (*api.QueryOptions)(nil)).Return((*api.SessionEntry)(nil), &api.QueryMeta{}, nil)
		// Second session creation
		mockSession.On("Create", mock.Anything, (*api.WriteOptions)(nil)).Return(secondSessionID, nil, nil).Once()
		mockSession.On("RenewPeriodic", mock.Anything, secondSessionID, (*api.WriteOptions)(nil), mock.Anything).Return(nil)

		mockClient := &MockConsulClient{}
		mockClient.On("Session").Return(mockSession)

		b := &Ballot{
			client: mockClient,
			TTL:    10 * time.Second,
			ctx:    context.Background(),
		}

		// Create first session
		err := b.session()
		assert.NoError(t, err)
		storedSessionID, ok := b.getSessionID()
		assert.True(t, ok)
		assert.Equal(t, firstSessionID, *storedSessionID)

		assert.NotNil(t, b.sessionLifecycle)

		// Create second session - should cancel the first renewal
		err = b.session()
		assert.NoError(t, err)
		storedSessionID, ok = b.getSessionID()
		assert.True(t, ok)
		assert.Equal(t, secondSessionID, *storedSessionID)
	})
}

func TestHandleServiceCriticalState(t *testing.T) {
	t.Run("service is found successfully", func(t *testing.T) {
		// Set up the mock Agent
		mockAgent := new(MockAgent)
		serviceID := "test_service_id"
		serviceName := "test_service"

		mockAgent.On("Service", serviceID, mock.Anything).Return(&api.AgentService{
			ID:      serviceID,
			Service: serviceName,
		}, nil, nil)

		// Set up the mock Catalog
		mockCatalog := new(MockCatalog)
		mockCatalog.On("Service", serviceName, "primary", mock.Anything).Return([]*api.CatalogService{
			{
				ServiceID:   serviceID,
				ServiceName: serviceName,
			},
		}, nil, nil)

		// Set up the mock ConsulClient
		mockClient := &MockConsulClient{}
		mockClient.On("Agent").Return(mockAgent)
		mockClient.On("Catalog").Return(mockCatalog)
		mockClient.On("Health").Return(new(MockHealth))
		mockClient.On("Session").Return(new(MockSession))
		mockClient.On("KV").Return(new(MockKV))

		b := &Ballot{
			client:     mockClient,
			ID:         serviceID,
			Name:       serviceName,
			PrimaryTag: "primary",
		}

		service, catalogServices, err := b.getService()
		assert.NoError(t, err)
		assert.NotNil(t, service)
		assert.NotNil(t, catalogServices)
		assert.Equal(t, serviceID, service.ID)
		assert.Equal(t, serviceName, service.Service)
		assert.Equal(t, 1, len(catalogServices))
		assert.Equal(t, serviceID, catalogServices[0].ServiceID)
	})

	t.Run("service is in passing state", func(t *testing.T) {
		mockHealth := new(MockHealth)
		mockHealth.On("Checks", "test_service", (*api.QueryOptions)(nil)).Return([]*api.HealthCheck{
			{Status: "passing"},
		}, nil, nil)

		mockClient := &MockConsulClient{}
		mockClient.On("Health").Return(mockHealth)

		b := &Ballot{
			client: mockClient,
			Name:   "test_service",
		}

		err := b.handleServiceCriticalState()
		assert.NoError(t, err)
	})

	t.Run("error occurs when getting health checks", func(t *testing.T) {
		expectedErr := fmt.Errorf("health check error")
		mockHealth := new(MockHealth)
		mockHealth.On("Checks", "test_service", (*api.QueryOptions)(nil)).Return(nil, nil, expectedErr)

		mockClient := &MockConsulClient{}
		mockClient.On("Health").Return(mockHealth)

		b := &Ballot{
			client: mockClient,
			Name:   "test_service",
		}

		err := b.handleServiceCriticalState()
		assert.Error(t, err)
		assert.ErrorContains(t, err, expectedErr.Error())
	})

	t.Run("filters checks by service ID", func(t *testing.T) {
		serviceID := "test_service_id"
		mockHealth := new(MockHealth)
		// Return multiple health checks, some for this instance and some for others
		mockHealth.On("Checks", "test_service", (*api.QueryOptions)(nil)).Return([]*api.HealthCheck{
			{ServiceID: serviceID, CheckID: "check1", Status: "passing"},
			{ServiceID: "other_service_id", CheckID: "check2", Status: "critical"}, // Different instance - should be ignored
		}, nil, nil)

		mockClient := &MockConsulClient{}
		mockClient.On("Health").Return(mockHealth)

		b := &Ballot{
			client: mockClient,
			ID:     serviceID,
			Name:   "test_service",
		}

		// Should pass because only our instance's checks are considered
		err := b.handleServiceCriticalState()
		assert.NoError(t, err)
	})

	t.Run("filters checks by service ID with critical state", func(t *testing.T) {
		serviceID := "test_service_id"
		serviceName := "test_service"
		primaryTag := "primary"

		mockHealth := new(MockHealth)
		mockSession := new(MockSession)
		mockAgent := new(MockAgent)
		mockCatalog := new(MockCatalog)

		// Return health checks where this instance is critical
		mockHealth.On("Checks", serviceName, (*api.QueryOptions)(nil)).Return([]*api.HealthCheck{
			{ServiceID: serviceID, CheckID: "check1", Status: "critical"},
			{ServiceID: "other_service_id", CheckID: "check2", Status: "passing"},
		}, nil, nil)

		sessionID := "session_id"
		mockSession.On("Destroy", sessionID, (*api.WriteOptions)(nil)).Return(nil, nil)

		// Mock Agent and Catalog for updateServiceTags
		mockAgent.On("Service", serviceID, mock.Anything).Return(&api.AgentService{
			ID:      serviceID,
			Service: serviceName,
			Tags:    []string{},
		}, nil, nil)
		mockCatalog.On("Service", serviceName, primaryTag, mock.Anything).Return([]*api.CatalogService{}, nil, nil)

		mockClient := &MockConsulClient{}
		mockClient.On("Health").Return(mockHealth)
		mockClient.On("Session").Return(mockSession)
		mockClient.On("Agent").Return(mockAgent)
		mockClient.On("Catalog").Return(mockCatalog)

		b := &Ballot{
			client:     mockClient,
			ID:         serviceID,
			Name:       serviceName,
			PrimaryTag: primaryTag,
		}
		b.sessionID.Store(&sessionID)

		err := b.handleServiceCriticalState()
		// Should return error because service is in critical state
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "service is in critical state")
		// Verify session was destroyed due to critical state
		mockSession.AssertCalled(t, "Destroy", sessionID, (*api.WriteOptions)(nil))
	})

	t.Run("filters checks by ServiceChecks list", func(t *testing.T) {
		serviceID := "test_service_id"
		mockHealth := new(MockHealth)
		// Return multiple health checks
		mockHealth.On("Checks", "test_service", (*api.QueryOptions)(nil)).Return([]*api.HealthCheck{
			{ServiceID: serviceID, CheckID: "check1", Status: "passing"},
			{ServiceID: serviceID, CheckID: "check2", Status: "critical"}, // Not in ServiceChecks - should be ignored
		}, nil, nil)

		mockClient := &MockConsulClient{}
		mockClient.On("Health").Return(mockHealth)

		b := &Ballot{
			client:        mockClient,
			ID:            serviceID,
			Name:          "test_service",
			ServiceChecks: []string{"check1"}, // Only consider check1
		}

		// Should pass because check2 (which is critical) is not in ServiceChecks
		err := b.handleServiceCriticalState()
		assert.NoError(t, err)
	})
}

func TestUpdateServiceTags(t *testing.T) {
	serviceID := "test_service_id"
	primaryTag := "primary"
	serviceName := "test_service"

	// Base service data without the primary tag
	baseService := &api.AgentService{
		ID:      serviceID,
		Service: serviceName,
		Tags:    []string{"tag1", "tag2"},
		Port:    8080,
		Address: "127.0.0.1",
	}

	// Subtest: Add primary tag when becoming leader
	t.Run("Add_primary_tag_when_becoming_leader", func(t *testing.T) {
		// Create fresh mocks for this subtest
		mockAgent := new(MockAgent)
		mockCatalog := new(MockCatalog)
		mockClient := &MockConsulClient{}
		mockClient.On("Agent").Return(mockAgent)
		mockClient.On("Catalog").Return(mockCatalog)

		// Mock the Agent().Service call to return the base service without the primary tag
		mockAgent.On("Service", serviceID, mock.Anything).Return(baseService, nil, nil)

		// Mock the Catalog().Service call to return an empty slice (no existing primary tag)
		mockCatalog.On("Service", serviceName, primaryTag, mock.AnythingOfType("*api.QueryOptions")).Return([]*api.CatalogService{}, nil, nil)

		// Mock the Agent().ServiceRegister to accept any *api.AgentServiceRegistration and return nil error
		mockAgent.On("ServiceRegister", mock.MatchedBy(func(reg *api.AgentServiceRegistration) bool {
			// Ensure the primary tag is added
			for _, tag := range reg.Tags {
				if tag == primaryTag {
					return true
				}
			}
			return false
		})).Return(nil)

		// Initialize the Ballot instance with the mock client
		b := &Ballot{
			client:        mockClient,
			ID:            serviceID,
			Name:          serviceName,
			PrimaryTag:    primaryTag,
			ctx:           context.Background(),
			ExecOnPromote: "",                 // Prevent running commands
			ExecOnDemote:  "",                 // Prevent running commands
			executor:      &commandExecutor{}, // Use the real executor or a mock if necessary
		}

		// Execute the method under test
		err := b.updateServiceTags(true)
		assert.NoError(t, err)

		// Assert that ServiceRegister was called once with the primary tag added
		mockAgent.AssertCalled(t, "ServiceRegister", mock.MatchedBy(func(reg *api.AgentServiceRegistration) bool {
			return slices.Contains(reg.Tags, primaryTag)
		}))
	})

	// Subtest: Remove primary tag when losing leadership
	t.Run("Remove_primary_tag_when_losing_leadership", func(t *testing.T) {
		// Clone the base service and add the primary tag
		serviceWithPrimary := &api.AgentService{
			ID:      serviceID,
			Service: serviceName,
			Tags:    append([]string{"tag1", "tag2"}, primaryTag),
			Port:    8080,
			Address: "127.0.0.1",
		}

		// Create fresh mocks for this subtest
		mockAgent := new(MockAgent)
		mockCatalog := new(MockCatalog)
		mockClient := &MockConsulClient{}
		mockClient.On("Agent").Return(mockAgent)
		mockClient.On("Catalog").Return(mockCatalog)

		// Mock the Agent().Service call to return the service with the primary tag
		mockAgent.On("Service", serviceID, mock.Anything).Return(serviceWithPrimary, nil, nil)

		// Mock the Catalog().Service call to return existing services with the primary tag
		mockCatalog.On("Service", serviceName, primaryTag, mock.AnythingOfType("*api.QueryOptions")).Return([]*api.CatalogService{
			{ServiceID: "other_service_id", ServiceName: serviceName, ServiceTags: []string{primaryTag}},
		}, nil, nil)

		// Mock the Agent().ServiceRegister to accept any *api.AgentServiceRegistration and return nil error
		mockAgent.On("ServiceRegister", mock.MatchedBy(func(reg *api.AgentServiceRegistration) bool {
			// Ensure the primary tag is removed
			return !slices.Contains(reg.Tags, primaryTag)
		})).Return(nil)

		// Initialize the Ballot instance with the mock client
		b := &Ballot{
			client:        mockClient,
			ID:            serviceID,
			Name:          serviceName,
			PrimaryTag:    primaryTag,
			ctx:           context.Background(),
			ExecOnPromote: "",                 // Prevent running commands
			ExecOnDemote:  "",                 // Prevent running commands
			executor:      &commandExecutor{}, // Use the real executor or a mock if necessary
		}

		// Execute the method under test
		err := b.updateServiceTags(false)
		assert.NoError(t, err)

		// Assert that ServiceRegister was called once with the primary tag removed
		mockAgent.AssertCalled(t, "ServiceRegister", mock.MatchedBy(func(reg *api.AgentServiceRegistration) bool {
			return !slices.Contains(reg.Tags, primaryTag)
		}))
	})

	// Subtest: No changes when tags are already correct
	t.Run("No_changes_when_tags_are_already_correct", func(t *testing.T) {
		// Clone the base service and add the primary tag
		serviceWithPrimary := &api.AgentService{
			ID:      serviceID,
			Service: serviceName,
			Tags:    append([]string{"tag1", "tag2"}, primaryTag),
			Port:    8080,
			Address: "127.0.0.1",
		}

		// Create fresh mocks for this subtest
		mockAgent := new(MockAgent)
		mockCatalog := new(MockCatalog)
		mockClient := &MockConsulClient{}
		mockClient.On("Agent").Return(mockAgent)
		mockClient.On("Catalog").Return(mockCatalog)

		// Mock the Agent().Service call to return the service with the primary tag
		mockAgent.On("Service", serviceID, mock.Anything).Return(serviceWithPrimary, nil, nil)

		// Mock the Catalog().Service call to return existing services without needing changes
		mockCatalog.On("Service", serviceName, primaryTag, mock.AnythingOfType("*api.QueryOptions")).Return([]*api.CatalogService{}, nil, nil)

		// No expectation for ServiceRegister since no changes should be made

		// Initialize the Ballot instance with the mock client
		b := &Ballot{
			client:        mockClient,
			ID:            serviceID,
			Name:          serviceName,
			PrimaryTag:    primaryTag,
			ctx:           context.Background(),
			ExecOnPromote: "",                 // Prevent running commands
			ExecOnDemote:  "",                 // Prevent running commands
			executor:      &commandExecutor{}, // Use the real executor or a mock if necessary
		}

		// Execute the method under test
		err := b.updateServiceTags(true)
		assert.NoError(t, err)

		// Assert that ServiceRegister was NOT called
		mockAgent.AssertNotCalled(t, "ServiceRegister")
	})

	// Subtest: Handle error when updating service registration fails
	t.Run("Handle_error_when_updating_service_registration_fails", func(t *testing.T) {
		// Create fresh mocks for this subtest
		mockAgent := new(MockAgent)
		mockCatalog := new(MockCatalog)
		mockClient := &MockConsulClient{}
		mockClient.On("Agent").Return(mockAgent)
		mockClient.On("Catalog").Return(mockCatalog)

		// Mock the Agent().Service call to return the base service without the primary tag
		mockAgent.On("Service", serviceID, mock.Anything).Return(baseService, nil, nil)

		// Mock the Catalog().Service call to return an empty slice (no existing primary tag)
		mockCatalog.On("Service", serviceName, primaryTag, mock.AnythingOfType("*api.QueryOptions")).Return([]*api.CatalogService{}, nil, nil)

		// Mock the Agent().ServiceRegister to return an error
		expectedErr := fmt.Errorf("failed to register service")
		mockAgent.On("ServiceRegister", mock.AnythingOfType("*api.AgentServiceRegistration")).Return(expectedErr)

		// Initialize the Ballot instance with the mock client
		b := &Ballot{
			client:        mockClient,
			ID:            serviceID,
			Name:          serviceName,
			PrimaryTag:    primaryTag,
			ctx:           context.Background(),
			ExecOnPromote: "",                 // Prevent running commands
			ExecOnDemote:  "",                 // Prevent running commands
			executor:      &commandExecutor{}, // Use the real executor or a mock if necessary
		}

		// Execute the method under test
		err := b.updateServiceTags(true)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)

		// Assert that ServiceRegister was called once with the primary tag added
		mockAgent.AssertCalled(t, "ServiceRegister", mock.MatchedBy(func(reg *api.AgentServiceRegistration) bool {
			return slices.Contains(reg.Tags, primaryTag)
		}))
	})
}

func TestCleanup(t *testing.T) {
	// Subtest: Successful cleanup when leader
	t.Run("Successful_cleanup_when_leader", func(t *testing.T) {
		primaryTag := "primary"
		serviceName := "test_service"

		// Define the leader's election payload
		leaderPayload := &ElectionPayload{
			Address:   "127.0.0.1",
			Port:      8080,
			SessionID: "session_id",
		}

		// Define another service that needs cleanup (has the primary tag)
		otherService := &api.CatalogService{
			ServiceID:      "other_service_id",
			ServiceName:    serviceName,
			ServiceTags:    []string{primaryTag, "tag1"},
			Node:           "node1",
			ServiceAddress: "127.0.0.2", // Different from leader's address
			ServicePort:    8081,        // Different from leader's port
		}

		// Create fresh mocks for this subtest
		mockCatalog := new(MockCatalog)
		mockClient := &MockConsulClient{}
		mockClient.On("Catalog").Return(mockCatalog)

		// Mock the Catalog().Service call to return the other service with the primary tag
		mockCatalog.On("Service", serviceName, "", mock.AnythingOfType("*api.QueryOptions")).Return([]*api.CatalogService{otherService}, nil, nil)

		// Mock the Catalog().Register to accept *api.CatalogRegistration with primary tag removed and return nil error
		mockCatalog.On("Register", mock.MatchedBy(func(reg *api.CatalogRegistration) bool {
			if reg.Service == nil {
				return false
			}
			return !slices.Contains(reg.Service.Tags, primaryTag)
		}), mock.Anything).Return(&api.WriteMeta{}, nil)

		// Initialize the Ballot instance with the mock client
		b := &Ballot{
			client:        mockClient,
			Name:          serviceName,
			PrimaryTag:    primaryTag,
			ctx:           context.Background(),
			ExecOnPromote: "",                 // Prevent running commands
			ExecOnDemote:  "",                 // Prevent running commands
			executor:      &commandExecutor{}, // Use the real executor or a mock if necessary
		}

		// Set the Ballot as leader and set sessionID
		sessionID := leaderPayload.SessionID
		b.leader.Store(true)
		b.sessionID.Store(&sessionID)

		// Execute the method under test
		err := b.cleanup(leaderPayload)
		assert.NoError(t, err)

		// Assert that Catalog().Register was called once with the primary tag removed
		mockCatalog.AssertCalled(t, "Register", mock.MatchedBy(func(reg *api.CatalogRegistration) bool {
			if reg.Service == nil {
				return false
			}
			return !slices.Contains(reg.Service.Tags, primaryTag)
		}), mock.Anything)
	})

	// Subtest: No cleanup when not leader
	t.Run("No_cleanup_when_not_leader", func(t *testing.T) {
		primaryTag := "primary"
		serviceName := "test_service"

		// Define the leader's election payload
		leaderPayload := &ElectionPayload{
			Address:   "127.0.0.1",
			Port:      8080,
			SessionID: "session_id",
		}

		// Create fresh mocks for this subtest
		mockCatalog := new(MockCatalog)
		mockClient := &MockConsulClient{}
		mockClient.On("Catalog").Return(mockCatalog)

		// Initialize the Ballot instance with the mock client
		b := &Ballot{
			client:        mockClient,
			Name:          serviceName,
			PrimaryTag:    primaryTag,
			ctx:           context.Background(),
			ExecOnPromote: "",                 // Prevent running commands
			ExecOnDemote:  "",                 // Prevent running commands
			executor:      &commandExecutor{}, // Use the real executor or a mock if necessary
		}

		// Set the Ballot as not leader
		b.leader.Store(false)

		// Execute the method under test
		err := b.cleanup(leaderPayload)
		assert.NoError(t, err)

		// Assert that Catalog().Register was NOT called since not leader
		mockCatalog.AssertNotCalled(t, "Register", mock.Anything, mock.Anything)
	})

	// Subtest: Handle error when updating catalog fails
	t.Run("Handle_error_when_updating_catalog_fails", func(t *testing.T) {
		primaryTag := "primary"
		serviceName := "test_service"

		// Define the leader's election payload
		leaderPayload := &ElectionPayload{
			Address:   "127.0.0.1",
			Port:      8080,
			SessionID: "session_id",
		}

		// Define another service that needs cleanup (has the primary tag)
		otherService := &api.CatalogService{
			ServiceID:      "other_service_id",
			ServiceName:    serviceName,
			ServiceTags:    []string{primaryTag, "tag1"},
			Node:           "node1",
			ServiceAddress: "127.0.0.2", // Different from leader's address
			ServicePort:    8081,        // Different from leader's port
		}

		// Create fresh mocks for this subtest
		mockCatalog := new(MockCatalog)
		mockClient := &MockConsulClient{}
		mockClient.On("Catalog").Return(mockCatalog)

		// Mock the Catalog().Service call to return the other service with the primary tag
		mockCatalog.On("Service", serviceName, "", mock.AnythingOfType("*api.QueryOptions")).Return([]*api.CatalogService{otherService}, nil, nil)

		// Expected error
		expectedErr := fmt.Errorf("failed to register catalog service")

		// Mock the Catalog().Register to return the expected error
		mockCatalog.On("Register", mock.AnythingOfType("*api.CatalogRegistration"), mock.AnythingOfType("*api.WriteOptions")).Run(func(args mock.Arguments) {
			t.Log("Catalog.Register was called")
			reg, ok := args.Get(0).(*api.CatalogRegistration)
			if !ok {
				t.Errorf("Register called with incorrect first argument type: %T", args.Get(0))
				return
			}
			t.Logf("Register called with: %+v", reg)
		}).Return(nil, expectedErr)

		// Initialize the Ballot instance with the mock client
		b := &Ballot{
			client:        mockClient,
			Name:          serviceName,
			PrimaryTag:    primaryTag,
			ctx:           context.Background(),
			ExecOnPromote: "", // Prevent running commands
			ExecOnDemote:  "", // Prevent running commands
		}

		// Set the Ballot as leader and set sessionID
		sessionID := leaderPayload.SessionID
		b.leader.Store(true)
		b.sessionID.Store(&sessionID)

		// Execute the method under test
		err := b.cleanup(leaderPayload)
		assert.Error(t, err)
		assert.EqualError(t, err, fmt.Sprintf("failed to update service tags in the catalog: %s", expectedErr))

		// Assert that Catalog().Register was called
		mockCatalog.AssertCalled(t, "Register", mock.AnythingOfType("*api.CatalogRegistration"), mock.AnythingOfType("*api.WriteOptions"))
	})
}

func TestAttemptLeadershipAcquisition(t *testing.T) {
	t.Run("Successful acquisition", func(t *testing.T) {
		// Create fresh Ballot instance
		sessionID := "session_id"
		b := &Ballot{
			Key: "election/test_service/leader",
		}
		b.sessionID.Store(&sessionID)

		payload := &ElectionPayload{
			Address:   "127.0.0.1",
			Port:      8080,
			SessionID: sessionID,
		}

		// Create fresh mocks
		mockKV := new(MockKV)
		mockKV.On("Acquire", mock.Anything, (*api.WriteOptions)(nil)).Return(true, &api.WriteMeta{}, nil)

		mockClient := &MockConsulClient{}
		mockClient.On("KV").Return(mockKV)

		// Assign the mock client to the Ballot instance
		b.client = mockClient

		// Execute the method under test
		acquired, _, err := b.attemptLeadershipAcquisition(payload)
		assert.NoError(t, err)
		assert.True(t, acquired)
	})

	t.Run("Failure due to nil session ID", func(t *testing.T) {
		// Create fresh Ballot instance with nil session ID
		b := &Ballot{
			Key: "election/test_service/leader",
		}
		b.sessionID.Store((*string)(nil))

		payload := &ElectionPayload{
			Address:   "127.0.0.1",
			Port:      8080,
			SessionID: "session_id",
		}

		// Create fresh mocks
		mockKV := new(MockKV)
		// No need to set up Acquire, as it should not be called

		mockClient := &MockConsulClient{}
		mockClient.On("KV").Return(mockKV)

		// Assign the mock client to the Ballot instance
		b.client = mockClient

		// Execute the method under test
		acquired, _, err := b.attemptLeadershipAcquisition(payload)
		assert.Error(t, err)
		assert.False(t, acquired)
	})

	t.Run("Failure due to KV Acquire error", func(t *testing.T) {
		// Create fresh Ballot instance
		sessionID := "session_id"
		b := &Ballot{
			Key: "election/test_service/leader",
		}
		b.sessionID.Store(&sessionID)

		payload := &ElectionPayload{
			Address:   "127.0.0.1",
			Port:      8080,
			SessionID: sessionID,
		}

		// Create fresh mocks
		mockKV := new(MockKV)
		expectedErr := fmt.Errorf("KV acquire error")
		mockKV.On("Acquire", mock.Anything, (*api.WriteOptions)(nil)).Return(false, nil, expectedErr)

		mockClient := &MockConsulClient{}
		mockClient.On("KV").Return(mockKV)

		// Assign the mock client to the Ballot instance
		b.client = mockClient

		// Execute the method under test
		acquired, _, err := b.attemptLeadershipAcquisition(payload)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.False(t, acquired)
	})
}

func TestVerifyAndUpdateLeadershipStatus(t *testing.T) {
	t.Run("Instance is leader", func(t *testing.T) {
		sessionID := "session_id"
		b := &Ballot{
			ID:         "test_service_id",
			Name:       "test_service",
			Key:        "election/test_service/leader",
			PrimaryTag: "primary",
		}
		b.sessionID.Store(&sessionID)

		payload := &ElectionPayload{
			SessionID: sessionID,
		}

		// Mock KV
		mockKV := new(MockKV)
		data, _ := json.Marshal(payload)
		mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(&api.KVPair{
			Key:     b.Key,
			Value:   data,
			Session: sessionID,
		}, nil, nil)

		// Mock Agent
		mockAgent := new(MockAgent)
		service := &api.AgentService{
			ID:      b.ID,
			Service: b.Name,
			Address: "127.0.0.1",
			Port:    8080,
			Tags:    []string{}, // initial tags
		}
		mockAgent.On("Service", b.ID, mock.Anything).Return(service, nil, nil)

		// **Set up expectation for ServiceRegister**
		mockAgent.On("ServiceRegister", mock.AnythingOfType("*api.AgentServiceRegistration")).Return(nil)

		// Mock Catalog
		mockCatalog := new(MockCatalog)
		mockCatalog.On("Service", b.Name, b.PrimaryTag, mock.AnythingOfType("*api.QueryOptions")).Return([]*api.CatalogService{}, nil, nil)

		// Mock Client
		mockClient := &MockConsulClient{}
		mockClient.On("KV").Return(mockKV)
		mockClient.On("Agent").Return(mockAgent)
		mockClient.On("Catalog").Return(mockCatalog)

		b.client = mockClient

		// Execute the method under test
		err := b.verifyAndUpdateLeadershipStatus()
		assert.NoError(t, err)
		assert.True(t, b.IsLeader())

		// Optionally, assert that ServiceRegister was called
		mockAgent.AssertCalled(t, "ServiceRegister", mock.AnythingOfType("*api.AgentServiceRegistration"))
	})

	t.Run("Instance is not leader", func(t *testing.T) {
		sessionID := "session_id"
		b := &Ballot{
			ID:         "test_service_id",
			Name:       "test_service",
			Key:        "election/test_service/leader",
			PrimaryTag: "primary",
		}
		b.sessionID.Store(&sessionID)

		otherSessionID := "other_session_id"
		payload := &ElectionPayload{
			SessionID: otherSessionID,
		}

		// Mock KV
		mockKV := new(MockKV)
		data, _ := json.Marshal(payload)
		mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(&api.KVPair{
			Key:     b.Key,
			Value:   data,
			Session: otherSessionID,
		}, nil, nil)

		// Mock Agent
		mockAgent := new(MockAgent)
		service := &api.AgentService{
			ID:      b.ID,
			Service: b.Name,
			Address: "127.0.0.1",
			Port:    8080,
		}
		mockAgent.On("Service", b.ID, mock.Anything).Return(service, nil, nil)

		// Mock Catalog
		mockCatalog := new(MockCatalog)
		mockCatalog.On("Service", b.Name, b.PrimaryTag, mock.AnythingOfType("*api.QueryOptions")).Return([]*api.CatalogService{}, nil, nil)

		// Mock Client
		mockClient := &MockConsulClient{}
		mockClient.On("KV").Return(mockKV)
		mockClient.On("Agent").Return(mockAgent)
		mockClient.On("Catalog").Return(mockCatalog)

		b.client = mockClient

		err := b.verifyAndUpdateLeadershipStatus()
		assert.NoError(t, err)
		assert.False(t, b.IsLeader())
	})

	t.Run("Error getting session data", func(t *testing.T) {
		sessionID := "session_id"
		b := &Ballot{
			ID:         "test_service_id",
			Name:       "test_service",
			Key:        "election/test_service/leader",
			PrimaryTag: "primary",
		}
		b.sessionID.Store(&sessionID)

		// Mock KV
		expectedErr := fmt.Errorf("KV get error")
		mockKV := new(MockKV)
		mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(nil, nil, expectedErr)

		// Mock Client
		mockClient := &MockConsulClient{}
		mockClient.On("KV").Return(mockKV)

		b.client = mockClient

		err := b.verifyAndUpdateLeadershipStatus()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), expectedErr.Error())
	})
}

func TestWaitForNextValidSessionData(t *testing.T) {
	t.Run("Successfully retrieves session data", func(t *testing.T) {
		b := &Ballot{
			Key: "election/test_service/leader",
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		payload := &ElectionPayload{
			SessionID: "session_id",
		}

		mockKV := new(MockKV)
		data, _ := json.Marshal(payload)
		mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(&api.KVPair{
			Key:   b.Key,
			Value: data,
		}, nil, nil)

		mockClient := &MockConsulClient{}
		mockClient.On("KV").Return(mockKV)

		b.client = mockClient

		// Correctly assign to payload of type *ElectionPayload
		resultPayload, err := b.waitForNextValidSessionData(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, resultPayload)
		assert.Equal(t, payload.SessionID, resultPayload.SessionID)
	})

	t.Run("Handles context cancellation", func(t *testing.T) {
		b := &Ballot{
			Key: "election/test_service/leader",
		}
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
		defer cancel()

		mockKV := new(MockKV)
		mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(nil, nil, nil)

		mockClient := &MockConsulClient{}
		mockClient.On("KV").Return(mockKV)

		b.client = mockClient

		// Correctly assign to payload of type *ElectionPayload
		resultPayload, err := b.waitForNextValidSessionData(ctx)
		assert.Error(t, err)
		assert.Equal(t, context.DeadlineExceeded, err)
		assert.Nil(t, resultPayload)
	})

	t.Run("Handles errors when getting session data", func(t *testing.T) {
		b := &Ballot{
			Key: "election/test_service/leader",
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		expectedErr := fmt.Errorf("KV get error")
		mockKV := new(MockKV)
		mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(nil, nil, expectedErr)

		mockClient := &MockConsulClient{}
		mockClient.On("KV").Return(mockKV)

		b.client = mockClient

		// Correctly assign to payload of type *ElectionPayload
		resultPayload, err := b.waitForNextValidSessionData(ctx)
		assert.Error(t, err)
		assert.ErrorContains(t, err, expectedErr.Error())
		assert.Nil(t, resultPayload)
	})
}

func TestGetSessionData(t *testing.T) {
	t.Run("Successfully retrieves session data", func(t *testing.T) {
		b := &Ballot{
			Key: "election/test_service/leader",
		}

		mockKV := new(MockKV)
		mockClient := &MockConsulClient{}
		mockClient.On("KV").Return(mockKV)
		b.client = mockClient

		payload := &ElectionPayload{
			SessionID: "session_id",
		}
		data, _ := json.Marshal(payload)
		mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(&api.KVPair{
			Key:   b.Key,
			Value: data,
		}, nil, nil)

		result, err := b.getSessionData()
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, payload.SessionID, result.SessionID)
	})

	t.Run("No data present", func(t *testing.T) {
		b := &Ballot{
			Key: "election/test_service/leader",
		}

		mockKV := new(MockKV)
		mockClient := &MockConsulClient{}
		mockClient.On("KV").Return(mockKV)
		b.client = mockClient

		mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(nil, nil, nil)

		result, err := b.getSessionData()
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("Error from KV store", func(t *testing.T) {
		b := &Ballot{
			Key: "election/test_service/leader",
		}

		mockKV := new(MockKV)
		mockClient := &MockConsulClient{}
		mockClient.On("KV").Return(mockKV)
		b.client = mockClient

		expectedErr := fmt.Errorf("KV get error")
		mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(nil, nil, expectedErr)

		result, err := b.getSessionData()
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Nil(t, result)
	})

	t.Run("JSON unmarshal error", func(t *testing.T) {
		b := &Ballot{
			Key: "election/test_service/leader",
		}

		mockKV := new(MockKV)
		mockClient := &MockConsulClient{}
		mockClient.On("KV").Return(mockKV)
		b.client = mockClient

		mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(&api.KVPair{
			Key:   b.Key,
			Value: []byte("invalid json"),
		}, nil, nil)

		result, err := b.getSessionData()
		assert.Error(t, err)
		assert.Nil(t, result)
	})
}

func TestElection(t *testing.T) {
	t.Run("Successful election", func(t *testing.T) {
		b := &Ballot{
			ID:         "test_service_id",
			Name:       "test_service",
			Key:        "election/test_service/leader",
			PrimaryTag: "primary",
			TTL:        10 * time.Second,
			ctx:        context.Background(),
		}

		// Mock health checks
		mockHealth := new(MockHealth)
		mockHealth.On("Checks", b.Name, mock.Anything).Return([]*api.HealthCheck{
			{Status: "passing"},
		}, nil, nil)

		// Mock session
		sessionID := "session_id"
		mockSession := new(MockSession)
		mockSession.On("Create", mock.Anything, mock.Anything).Return(sessionID, nil, nil)
		mockSession.On("RenewPeriodic", mock.Anything, sessionID, mock.Anything, mock.Anything).Return(nil)
		mockSession.On("Info", sessionID, mock.Anything).Return(&api.SessionEntry{ID: sessionID}, nil, nil)

		// Mock KV
		payload := &ElectionPayload{
			Address:   "127.0.0.1",
			Port:      8080,
			SessionID: sessionID,
		}
		data, _ := json.Marshal(payload)
		mockKV := new(MockKV)
		mockKV.On("Acquire", mock.Anything, mock.Anything).Return(true, nil, nil)
		mockKV.On("Get", b.Key, mock.Anything).Return(&api.KVPair{
			Key:     b.Key,
			Value:   data,
			Session: sessionID,
		}, nil, nil)

		// Mock Agent
		service := &api.AgentService{
			ID:      b.ID,
			Service: b.Name,
			Address: "127.0.0.1",
			Port:    8080,
			Tags:    []string{},
		}
		mockAgent := new(MockAgent)
		mockAgent.On("Service", b.ID, mock.Anything).Return(service, nil, nil)
		mockAgent.On("ServiceRegister", mock.Anything).Return(nil)

		// Mock Catalog
		mockCatalog := new(MockCatalog)
		mockCatalog.On("Service", b.Name, b.PrimaryTag, mock.Anything).Return([]*api.CatalogService{}, nil, nil)
		mockCatalog.On("Service", b.Name, "", mock.Anything).Return([]*api.CatalogService{}, nil, nil)
		mockCatalog.On("Register", mock.Anything, mock.Anything).Return(nil, nil)

		// Set up the mock client
		mockClient := &MockConsulClient{}
		mockClient.On("Health").Return(mockHealth)
		mockClient.On("Session").Return(mockSession)
		mockClient.On("KV").Return(mockKV)
		mockClient.On("Agent").Return(mockAgent)
		mockClient.On("Catalog").Return(mockCatalog)

		b.client = mockClient

		// Execute the method under test
		err := b.election()
		assert.NoError(t, err)

		// Add logging to confirm leader status
		t.Logf("Leader status after election: %v", b.IsLeader())

		// Verify that b.IsLeader() returns true
		assert.True(t, b.IsLeader(), "Expected b.IsLeader() to return true after successful election")
	})
}

func TestRun(t *testing.T) {
	t.Run("Run exits on context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		sessionID := "session_id"
		b := &Ballot{
			ID:         "test_service_id",
			Name:       "test_service",
			Key:        "election/test_service/leader",
			PrimaryTag: "primary",
			TTL:        100 * time.Millisecond,
			ctx:        ctx,
		}
		b.sessionID.Store(&sessionID)

		// Mock health checks
		mockHealth := new(MockHealth)
		mockHealth.On("Checks", b.Name, mock.Anything).Return([]*api.HealthCheck{
			{Status: "passing"},
		}, nil, nil)

		// Mock session
		mockSession := new(MockSession)
		mockSession.On("Create", mock.Anything, mock.Anything).Return(sessionID, nil, nil)
		mockSession.On("RenewPeriodic", mock.Anything, sessionID, mock.Anything, mock.Anything).Return(nil)
		mockSession.On("Info", sessionID, mock.Anything).Return(&api.SessionEntry{ID: sessionID}, &api.QueryMeta{}, nil)

		// Mock KV
		payload := &ElectionPayload{
			Address:   "127.0.0.1",
			Port:      8080,
			SessionID: sessionID,
		}
		data, _ := json.Marshal(payload)
		mockKV := new(MockKV)
		mockKV.On("Acquire", mock.Anything, mock.Anything).Return(true, nil, nil)
		mockKV.On("Get", b.Key, mock.Anything).Return(&api.KVPair{
			Key:     b.Key,
			Value:   data,
			Session: sessionID,
		}, nil, nil)

		// Mock Agent
		service := &api.AgentService{
			ID:      b.ID,
			Service: b.Name,
			Address: "127.0.0.1",
			Port:    8080,
			Tags:    []string{},
		}
		mockAgent := new(MockAgent)
		mockAgent.On("Service", b.ID, mock.Anything).Return(service, nil, nil)
		mockAgent.On("ServiceRegister", mock.Anything).Return(nil)

		// Mock Catalog
		mockCatalog := new(MockCatalog)
		mockCatalog.On("Service", b.Name, b.PrimaryTag, mock.Anything).Return([]*api.CatalogService{}, nil, nil)
		mockCatalog.On("Service", b.Name, "", mock.Anything).Return([]*api.CatalogService{}, nil, nil)
		mockCatalog.On("Register", mock.Anything, mock.Anything).Return(nil, nil)

		mockClient := &MockConsulClient{}
		mockClient.On("Health").Return(mockHealth)
		mockClient.On("Session").Return(mockSession)
		mockClient.On("KV").Return(mockKV)
		mockClient.On("Agent").Return(mockAgent)
		mockClient.On("Catalog").Return(mockCatalog)

		b.client = mockClient

		// Run in goroutine and cancel after short delay
		done := make(chan error, 1)
		go func() {
			done <- b.Run()
		}()

		// Cancel after letting it run briefly
		time.Sleep(150 * time.Millisecond)
		cancel()

		err := <-done
		assert.NoError(t, err)
	})
}

func TestGetService_EmptyServiceID(t *testing.T) {
	b := &Ballot{
		ID: "",
	}

	service, catalogServices, err := b.getService()
	assert.Error(t, err)
	assert.Nil(t, service)
	assert.Nil(t, catalogServices)
	assert.Contains(t, err.Error(), "service ID is empty")
}

func TestGetService_AgentServiceError(t *testing.T) {
	mockAgent := new(MockAgent)
	expectedErr := errors.New("agent service error")
	mockAgent.On("Service", "test_id", mock.Anything).Return(nil, nil, expectedErr)

	mockClient := &MockConsulClient{}
	mockClient.On("Agent").Return(mockAgent)

	b := &Ballot{
		ID:     "test_id",
		client: mockClient,
	}

	service, catalogServices, err := b.getService()
	assert.Error(t, err)
	assert.Nil(t, service)
	assert.Nil(t, catalogServices)
	assert.Equal(t, expectedErr, err)
}

func TestGetService_CatalogServiceError(t *testing.T) {
	serviceID := "test_service_id"
	serviceName := "test_service"

	mockAgent := new(MockAgent)
	mockAgent.On("Service", serviceID, mock.Anything).Return(&api.AgentService{
		ID:      serviceID,
		Service: serviceName,
	}, nil, nil)

	expectedErr := errors.New("catalog service error")
	mockCatalog := new(MockCatalog)
	mockCatalog.On("Service", serviceName, "primary", mock.Anything).Return(nil, nil, expectedErr)

	mockClient := &MockConsulClient{}
	mockClient.On("Agent").Return(mockAgent)
	mockClient.On("Catalog").Return(mockCatalog)

	b := &Ballot{
		ID:         serviceID,
		Name:       serviceName,
		PrimaryTag: "primary",
		client:     mockClient,
	}

	service, catalogServices, err := b.getService()
	assert.Error(t, err)
	assert.NotNil(t, service)
	assert.Nil(t, catalogServices)
	assert.Equal(t, expectedErr, err)
}

func TestUpdateServiceTags_GetServiceError(t *testing.T) {
	mockAgent := new(MockAgent)
	expectedErr := errors.New("get service error")
	mockAgent.On("Service", "test_id", mock.Anything).Return(nil, nil, expectedErr)

	mockClient := &MockConsulClient{}
	mockClient.On("Agent").Return(mockAgent)

	b := &Ballot{
		ID:     "test_id",
		client: mockClient,
	}

	err := b.updateServiceTags(true)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

func TestSession_ClientNil(t *testing.T) {
	b := &Ballot{
		client: nil,
	}

	err := b.session()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "consul client is required")
}

func TestSession_ExistingValidSession(t *testing.T) {
	sessionID := "existing_session"

	mockSession := new(MockSession)
	mockSession.On("Info", sessionID, (*api.QueryOptions)(nil)).Return(&api.SessionEntry{ID: sessionID}, &api.QueryMeta{}, nil)

	mockClient := &MockConsulClient{}
	mockClient.On("Session").Return(mockSession)

	b := &Ballot{
		client: mockClient,
		TTL:    10 * time.Second,
		ctx:    context.Background(),
	}
	b.sessionID.Store(&sessionID)

	err := b.session()
	assert.NoError(t, err)

	// Verify we didn't create a new session
	mockSession.AssertNotCalled(t, "Create", mock.Anything, mock.Anything)
}

func TestSession_InfoError(t *testing.T) {
	sessionID := "session_with_error"

	expectedErr := errors.New("info error")
	mockSession := new(MockSession)
	mockSession.On("Info", sessionID, (*api.QueryOptions)(nil)).Return((*api.SessionEntry)(nil), (*api.QueryMeta)(nil), expectedErr)

	mockClient := &MockConsulClient{}
	mockClient.On("Session").Return(mockSession)

	b := &Ballot{
		client: mockClient,
		TTL:    10 * time.Second,
		ctx:    context.Background(),
	}
	b.sessionID.Store(&sessionID)

	err := b.session()
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

func TestElection_HandleCriticalStateError(t *testing.T) {
	b := &Ballot{
		ID:   "test_service_id",
		Name: "test_service",
	}

	mockHealth := new(MockHealth)
	expectedErr := errors.New("health check error")
	mockHealth.On("Checks", b.Name, mock.Anything).Return(nil, nil, expectedErr)

	mockClient := &MockConsulClient{}
	mockClient.On("Health").Return(mockHealth)

	b.client = mockClient

	err := b.election()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
}

func TestElection_GetServiceError(t *testing.T) {
	b := &Ballot{
		ID:   "test_service_id",
		Name: "test_service",
	}

	mockHealth := new(MockHealth)
	mockHealth.On("Checks", b.Name, mock.Anything).Return([]*api.HealthCheck{
		{Status: "passing"},
	}, nil, nil)

	mockAgent := new(MockAgent)
	expectedErr := errors.New("agent service error")
	mockAgent.On("Service", b.ID, mock.Anything).Return(nil, nil, expectedErr)

	mockClient := &MockConsulClient{}
	mockClient.On("Health").Return(mockHealth)
	mockClient.On("Agent").Return(mockAgent)

	b.client = mockClient

	err := b.election()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get service")
}

func TestElection_SessionError(t *testing.T) {
	b := &Ballot{
		ID:   "test_service_id",
		Name: "test_service",
		TTL:  10 * time.Second,
		ctx:  context.Background(),
	}

	mockHealth := new(MockHealth)
	mockHealth.On("Checks", b.Name, mock.Anything).Return([]*api.HealthCheck{
		{Status: "passing"},
	}, nil, nil)

	mockAgent := new(MockAgent)
	mockAgent.On("Service", b.ID, mock.Anything).Return(&api.AgentService{
		ID:      b.ID,
		Service: b.Name,
	}, nil, nil)

	mockCatalog := new(MockCatalog)
	mockCatalog.On("Service", b.Name, "", mock.Anything).Return([]*api.CatalogService{}, nil, nil)

	expectedErr := errors.New("session create error")
	mockSession := new(MockSession)
	mockSession.On("Create", mock.Anything, mock.Anything).Return("", nil, expectedErr)

	mockClient := &MockConsulClient{}
	mockClient.On("Health").Return(mockHealth)
	mockClient.On("Agent").Return(mockAgent)
	mockClient.On("Catalog").Return(mockCatalog)
	mockClient.On("Session").Return(mockSession)

	b.client = mockClient

	err := b.election()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create session")
}

func TestElection_NilSessionID(t *testing.T) {
	sessionID := "session_id"
	b := &Ballot{
		ID:         "test_service_id",
		Name:       "test_service",
		Key:        "election/test/leader",
		PrimaryTag: "primary",
		TTL:        10 * time.Second,
		ctx:        context.Background(),
	}

	mockHealth := new(MockHealth)
	mockHealth.On("Checks", b.Name, mock.Anything).Return([]*api.HealthCheck{
		{Status: "passing"},
	}, nil, nil)

	mockAgent := new(MockAgent)
	mockAgent.On("Service", b.ID, mock.Anything).Return(&api.AgentService{
		ID:      b.ID,
		Service: b.Name,
	}, nil, nil)
	mockAgent.On("ServiceRegister", mock.Anything).Return(nil)

	mockCatalog := new(MockCatalog)
	mockCatalog.On("Service", b.Name, "", mock.Anything).Return([]*api.CatalogService{}, nil, nil)
	mockCatalog.On("Service", b.Name, b.PrimaryTag, mock.Anything).Return([]*api.CatalogService{}, nil, nil)

	// Session creation returns a valid session ID
	mockSession := new(MockSession)
	mockSession.On("Create", mock.Anything, mock.Anything).Return(sessionID, nil, nil)
	mockSession.On("RenewPeriodic", mock.Anything, sessionID, mock.Anything, mock.Anything).Return(nil)

	// Mock KV
	payload := &ElectionPayload{
		Address:   "",
		Port:      0,
		SessionID: sessionID,
	}
	data, _ := json.Marshal(payload)
	mockKV := new(MockKV)
	mockKV.On("Acquire", mock.Anything, mock.Anything).Return(true, nil, nil)
	mockKV.On("Get", b.Key, mock.Anything).Return(&api.KVPair{
		Key:     b.Key,
		Value:   data,
		Session: sessionID,
	}, nil, nil)

	mockClient := &MockConsulClient{}
	mockClient.On("Health").Return(mockHealth)
	mockClient.On("Agent").Return(mockAgent)
	mockClient.On("Catalog").Return(mockCatalog)
	mockClient.On("Session").Return(mockSession)
	mockClient.On("KV").Return(mockKV)

	b.client = mockClient

	err := b.election()
	assert.NoError(t, err)
}

func TestCleanup_CatalogError(t *testing.T) {
	primaryTag := "primary"
	serviceName := "test_service"

	leaderPayload := &ElectionPayload{
		Address:   "127.0.0.1",
		Port:      8080,
		SessionID: "session_id",
	}

	mockCatalog := new(MockCatalog)
	expectedErr := errors.New("catalog error")
	mockCatalog.On("Service", serviceName, "", mock.Anything).Return(nil, nil, expectedErr)

	mockClient := &MockConsulClient{}
	mockClient.On("Catalog").Return(mockCatalog)

	b := &Ballot{
		client:     mockClient,
		Name:       serviceName,
		PrimaryTag: primaryTag,
	}

	sessionID := leaderPayload.SessionID
	b.leader.Store(true)
	b.sessionID.Store(&sessionID)

	err := b.cleanup(leaderPayload)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to retrieve services from the catalog")
}

func TestVerifyAndUpdateLeadershipStatus_NilSessionData(t *testing.T) {
	sessionID := "session_id"
	b := &Ballot{
		Key: "election/test/leader",
	}
	b.sessionID.Store(&sessionID)

	mockKV := new(MockKV)
	mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(nil, nil, nil)

	mockClient := &MockConsulClient{}
	mockClient.On("KV").Return(mockKV)

	b.client = mockClient

	err := b.verifyAndUpdateLeadershipStatus()
	assert.NoError(t, err)
}

func TestHandleServiceCriticalState_WarningState(t *testing.T) {
	serviceID := "test_service_id"
	serviceName := "test_service"

	mockHealth := new(MockHealth)
	mockHealth.On("Checks", serviceName, (*api.QueryOptions)(nil)).Return([]*api.HealthCheck{
		{ServiceID: serviceID, CheckID: "check1", Status: "warning"},
	}, nil, nil)

	mockClient := &MockConsulClient{}
	mockClient.On("Health").Return(mockHealth)

	b := &Ballot{
		client: mockClient,
		ID:     serviceID,
		Name:   serviceName,
	}

	err := b.handleServiceCriticalState()
	assert.NoError(t, err)
}

func TestHandleServiceCriticalState_ErrorPaths(t *testing.T) {
	t.Run("returns release error", func(t *testing.T) {
		serviceID := "test_service_id"
		serviceName := "test_service"
		sessionID := "session_id"
		expectedErr := errors.New("destroy failed")

		mockHealth := new(MockHealth)
		mockHealth.On("Checks", serviceName, (*api.QueryOptions)(nil)).Return(api.HealthChecks{
			{ServiceID: serviceID, CheckID: "check1", Status: "critical"},
		}, nil, nil)
		mockSession := new(MockSession)
		mockSession.On("Destroy", sessionID, (*api.WriteOptions)(nil)).Return(nil, expectedErr)

		mockClient := &MockConsulClient{}
		mockClient.On("Health").Return(mockHealth)
		mockClient.On("Session").Return(mockSession)

		b := &Ballot{
			client: mockClient,
			ID:     serviceID,
			Name:   serviceName,
		}
		b.sessionID.Store(&sessionID)

		err := b.handleServiceCriticalState()

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to release session")
	})

	t.Run("returns leadership status update error", func(t *testing.T) {
		serviceID := "test_service_id"
		serviceName := "test_service"
		sessionID := "session_id"
		expectedErr := errors.New("agent failed")

		mockHealth := new(MockHealth)
		mockHealth.On("Checks", serviceName, (*api.QueryOptions)(nil)).Return(api.HealthChecks{
			{ServiceID: serviceID, CheckID: "check1", Status: "critical"},
		}, nil, nil)
		mockSession := new(MockSession)
		mockSession.On("Destroy", sessionID, (*api.WriteOptions)(nil)).Return(nil, nil)
		mockAgent := new(MockAgent)
		mockAgent.On("Service", serviceID, mock.Anything).Return(nil, nil, expectedErr)

		mockClient := &MockConsulClient{}
		mockClient.On("Health").Return(mockHealth)
		mockClient.On("Session").Return(mockSession)
		mockClient.On("Agent").Return(mockAgent)

		b := &Ballot{
			client:     mockClient,
			ID:         serviceID,
			Name:       serviceName,
			PrimaryTag: "primary",
		}
		b.sessionID.Store(&sessionID)

		err := b.handleServiceCriticalState()

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to update leadership status")
	})
}

func TestCommandExecutor(t *testing.T) {
	t.Run("CommandContext creates exec.Cmd", func(t *testing.T) {
		executor := &commandExecutor{}
		ctx := context.Background()

		cmd := executor.CommandContext(ctx, "echo", "hello")
		assert.NotNil(t, cmd)
		// Path is resolved to full path by exec.LookPath, check it contains "echo"
		assert.Contains(t, cmd.Path, "echo")
		assert.Contains(t, cmd.Args, "echo")
		assert.Contains(t, cmd.Args, "hello")
	})

	t.Run("CommandContext with no args", func(t *testing.T) {
		executor := &commandExecutor{}
		ctx := context.Background()

		cmd := executor.CommandContext(ctx, "true")
		assert.NotNil(t, cmd)
	})
}

func TestUpdateServiceTags_WithCommandExecution(t *testing.T) {
	t.Run("Executes ExecOnPromote when becoming leader", func(t *testing.T) {
		serviceID := "test_service_id"
		serviceName := "test_service"
		primaryTag := "primary"
		sessionID := "session_id"

		mockAgent := new(MockAgent)
		mockCatalog := new(MockCatalog)
		mockKV := new(MockKV)
		mockClient := &MockConsulClient{}

		// Service without primary tag
		baseService := &api.AgentService{
			ID:      serviceID,
			Service: serviceName,
			Tags:    []string{"tag1"},
			Port:    8080,
			Address: "127.0.0.1",
		}

		mockAgent.On("Service", serviceID, mock.Anything).Return(baseService, nil, nil)
		mockCatalog.On("Service", serviceName, primaryTag, mock.Anything).Return([]*api.CatalogService{}, nil, nil)
		mockAgent.On("ServiceRegister", mock.Anything).Return(nil)

		// Mock KV for session data retrieval
		payload := &ElectionPayload{
			Address:   "127.0.0.1",
			Port:      8080,
			SessionID: sessionID,
		}
		data, _ := json.Marshal(payload)
		mockKV.On("Get", "election/test/leader", mock.Anything).Return(&api.KVPair{
			Key:   "election/test/leader",
			Value: data,
		}, nil, nil)

		mockClient.On("Agent").Return(mockAgent)
		mockClient.On("Catalog").Return(mockCatalog)
		mockClient.On("KV").Return(mockKV)

		// Use a mock executor that tracks calls
		mockExecutor := new(MockCommandExecutor)
		mockCmd := exec.Command("echo", "promoted")
		mockExecutor.On("CommandContext", mock.Anything, "echo", []string{"promoted"}).Return(mockCmd)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		b := &Ballot{
			client:        mockClient,
			ID:            serviceID,
			Name:          serviceName,
			PrimaryTag:    primaryTag,
			Key:           "election/test/leader",
			ctx:           ctx,
			ExecOnPromote: "echo promoted",
			executor:      mockExecutor,
			TTL:           10 * time.Second,
			LockDelay:     3 * time.Second,
		}
		b.sessionID.Store(&sessionID)

		err := b.updateServiceTags(true)
		assert.NoError(t, err)

		result, ok := observeHookResult(t, b)
		assert.True(t, ok)
		assert.NoError(t, result.Err)
		assert.Equal(t, HookPromote, result.Transition)

		mockAgent.AssertCalled(t, "ServiceRegister", mock.Anything)
	})

	t.Run("Executes ExecOnDemote when losing leadership", func(t *testing.T) {
		serviceID := "test_service_id"
		serviceName := "test_service"
		primaryTag := "primary"
		sessionID := "session_id"

		mockAgent := new(MockAgent)
		mockCatalog := new(MockCatalog)
		mockKV := new(MockKV)
		mockClient := &MockConsulClient{}

		// Service with primary tag
		serviceWithTag := &api.AgentService{
			ID:      serviceID,
			Service: serviceName,
			Tags:    []string{"tag1", primaryTag},
			Port:    8080,
			Address: "127.0.0.1",
		}

		mockAgent.On("Service", serviceID, mock.Anything).Return(serviceWithTag, nil, nil)
		mockCatalog.On("Service", serviceName, primaryTag, mock.Anything).Return([]*api.CatalogService{}, nil, nil)
		mockAgent.On("ServiceRegister", mock.Anything).Return(nil)

		// Mock KV for session data retrieval
		payload := &ElectionPayload{
			Address:   "127.0.0.1",
			Port:      8080,
			SessionID: sessionID,
		}
		data, _ := json.Marshal(payload)
		mockKV.On("Get", "election/test/leader", mock.Anything).Return(&api.KVPair{
			Key:   "election/test/leader",
			Value: data,
		}, nil, nil)

		mockClient.On("Agent").Return(mockAgent)
		mockClient.On("Catalog").Return(mockCatalog)
		mockClient.On("KV").Return(mockKV)

		// Use a mock executor
		mockExecutor := new(MockCommandExecutor)
		mockCmd := exec.Command("echo", "demoted")
		mockExecutor.On("CommandContext", mock.Anything, "echo", []string{"demoted"}).Return(mockCmd)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		b := &Ballot{
			client:       mockClient,
			ID:           serviceID,
			Name:         serviceName,
			PrimaryTag:   primaryTag,
			Key:          "election/test/leader",
			ctx:          ctx,
			ExecOnDemote: "echo demoted",
			executor:     mockExecutor,
			TTL:          10 * time.Second,
			LockDelay:    3 * time.Second,
		}
		b.sessionID.Store(&sessionID)

		err := b.updateServiceTags(false)
		assert.NoError(t, err)

		result, ok := observeHookResult(t, b)
		assert.True(t, ok)
		assert.NoError(t, result.Err)
		assert.Equal(t, HookDemote, result.Transition)

		mockAgent.AssertCalled(t, "ServiceRegister", mock.Anything)
	})

	t.Run("Handles command execution error gracefully", func(t *testing.T) {
		serviceID := "test_service_id"
		serviceName := "test_service"
		primaryTag := "primary"
		sessionID := "session_id"

		mockAgent := new(MockAgent)
		mockCatalog := new(MockCatalog)
		mockKV := new(MockKV)
		mockClient := &MockConsulClient{}

		baseService := &api.AgentService{
			ID:      serviceID,
			Service: serviceName,
			Tags:    []string{"tag1"},
			Port:    8080,
			Address: "127.0.0.1",
		}

		mockAgent.On("Service", serviceID, mock.Anything).Return(baseService, nil, nil)
		mockCatalog.On("Service", serviceName, primaryTag, mock.Anything).Return([]*api.CatalogService{}, nil, nil)
		mockAgent.On("ServiceRegister", mock.Anything).Return(nil)

		// Mock KV for session data retrieval
		payload := &ElectionPayload{
			Address:   "127.0.0.1",
			Port:      8080,
			SessionID: sessionID,
		}
		data, _ := json.Marshal(payload)
		mockKV.On("Get", "election/test/leader", mock.Anything).Return(&api.KVPair{
			Key:   "election/test/leader",
			Value: data,
		}, nil, nil)

		mockClient.On("Agent").Return(mockAgent)
		mockClient.On("Catalog").Return(mockCatalog)
		mockClient.On("KV").Return(mockKV)

		// Use a mock executor that returns a failing command
		mockExecutor := new(MockCommandExecutor)
		mockCmd := exec.Command("false") // 'false' command exits with code 1
		mockExecutor.On("CommandContext", mock.Anything, "false", []string{}).Return(mockCmd)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		b := &Ballot{
			client:        mockClient,
			ID:            serviceID,
			Name:          serviceName,
			PrimaryTag:    primaryTag,
			Key:           "election/test/leader",
			ctx:           ctx,
			ExecOnPromote: "false",
			executor:      mockExecutor,
			TTL:           10 * time.Second,
			LockDelay:     3 * time.Second,
		}
		b.sessionID.Store(&sessionID)

		// Should not return error even if command fails
		err := b.updateServiceTags(true)
		assert.NoError(t, err)

		result, ok := observeHookResult(t, b)
		assert.True(t, ok)
		assert.Error(t, result.Err)
	})

	t.Run("Handles session data retrieval error in command goroutine", func(t *testing.T) {
		serviceID := "test_service_id"
		serviceName := "test_service"
		primaryTag := "primary"
		sessionID := "session_id"

		mockAgent := new(MockAgent)
		mockCatalog := new(MockCatalog)
		mockKV := new(MockKV)
		mockClient := &MockConsulClient{}

		baseService := &api.AgentService{
			ID:      serviceID,
			Service: serviceName,
			Tags:    []string{"tag1"},
			Port:    8080,
			Address: "127.0.0.1",
		}

		mockAgent.On("Service", serviceID, mock.Anything).Return(baseService, nil, nil)
		mockCatalog.On("Service", serviceName, primaryTag, mock.Anything).Return([]*api.CatalogService{}, nil, nil)
		mockAgent.On("ServiceRegister", mock.Anything).Return(nil)

		// Mock KV to return error
		mockKV.On("Get", "election/test/leader", mock.Anything).Return(nil, nil, errors.New("kv error"))

		mockClient.On("Agent").Return(mockAgent)
		mockClient.On("Catalog").Return(mockCatalog)
		mockClient.On("KV").Return(mockKV)

		mockExecutor := new(MockCommandExecutor)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		b := &Ballot{
			client:        mockClient,
			ID:            serviceID,
			Name:          serviceName,
			PrimaryTag:    primaryTag,
			Key:           "election/test/leader",
			ctx:           ctx,
			ExecOnPromote: "echo test",
			executor:      mockExecutor,
			TTL:           10 * time.Millisecond,
			LockDelay:     3 * time.Millisecond,
		}
		b.sessionID.Store(&sessionID)

		err := b.updateServiceTags(true)
		assert.NoError(t, err)

		result, ok := observeHookResult(t, b)
		assert.True(t, ok)
		assert.ErrorContains(t, result.Err, "kv error")
	})
}

func TestExecuteLeadershipHook_DefaultContext(t *testing.T) {
	sessionID := "session_id"
	key := "election/test/leader"
	payload := &ElectionPayload{
		Address:   "127.0.0.1",
		Port:      8080,
		SessionID: sessionID,
	}
	data, _ := json.Marshal(payload)

	mockKV := new(MockKV)
	mockKV.On("Get", key, (*api.QueryOptions)(nil)).Return(&api.KVPair{
		Key:   key,
		Value: data,
	}, nil, nil)

	mockClient := &MockConsulClient{}
	mockClient.On("KV").Return(mockKV)

	mockExecutor := new(MockCommandExecutor)
	mockExecutor.On("CommandContext", mock.Anything, "echo", []string{"ok"}).Return(exec.Command("echo", "ok"))

	b := &Ballot{
		client:    mockClient,
		Key:       key,
		executor:  mockExecutor,
		TTL:       10 * time.Second,
		LockDelay: 3 * time.Second,
	}
	b.sessionID.Store(&sessionID)

	result := b.executeLeadershipHook(true, "echo ok")

	assert.NoError(t, result.Err)
	assert.Equal(t, HookPromote, result.Transition)
	mockExecutor.AssertExpectations(t)
}

func TestReleaseSession_UsesLifecycle(t *testing.T) {
	sessionID := "session_id"
	mockSession := new(MockSession)
	mockSession.On("Destroy", sessionID, (*api.WriteOptions)(nil)).Return(&api.WriteMeta{}, nil)

	b := &Ballot{
		TTL:       10 * time.Second,
		LockDelay: 3 * time.Second,
		ctx:       context.Background(),
	}
	b.sessionID.Store(&sessionID)
	b.sessionLifecycle = NewSessionLifecycle(b.ctx, mockSession, &b.sessionID, b.runtimeConfig())

	err := b.releaseSession()

	assert.NoError(t, err)
	mockSession.AssertCalled(t, "Destroy", sessionID, (*api.WriteOptions)(nil))
}

func observeHookResult(t *testing.T, b *Ballot) (HookResult, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case result := <-b.hookResultsChan():
		return result, true
	case <-ctx.Done():
		return HookResult{Err: ctx.Err()}, false
	}
}

func TestRun_SmallTTL(t *testing.T) {
	t.Run("Run with TTL less than 2 seconds uses 1 second interval", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		sessionID := "session_id"
		b := &Ballot{
			ID:         "test_service_id",
			Name:       "test_service",
			Key:        "election/test_service/leader",
			PrimaryTag: "primary",
			TTL:        500 * time.Millisecond, // TTL/2 = 250ms < 1s, so interval becomes 1s
			ctx:        ctx,
		}
		b.sessionID.Store(&sessionID)

		mockHealth := new(MockHealth)
		mockHealth.On("Checks", b.Name, mock.Anything).Return([]*api.HealthCheck{
			{Status: "passing"},
		}, nil, nil)

		mockSession := new(MockSession)
		mockSession.On("Create", mock.Anything, mock.Anything).Return(sessionID, nil, nil)
		mockSession.On("RenewPeriodic", mock.Anything, sessionID, mock.Anything, mock.Anything).Return(nil)
		mockSession.On("Info", sessionID, mock.Anything).Return(&api.SessionEntry{ID: sessionID}, &api.QueryMeta{}, nil)

		payload := &ElectionPayload{
			Address:   "127.0.0.1",
			Port:      8080,
			SessionID: sessionID,
		}
		data, _ := json.Marshal(payload)
		mockKV := new(MockKV)
		mockKV.On("Acquire", mock.Anything, mock.Anything).Return(true, nil, nil)
		mockKV.On("Get", b.Key, mock.Anything).Return(&api.KVPair{
			Key:     b.Key,
			Value:   data,
			Session: sessionID,
		}, nil, nil)

		service := &api.AgentService{
			ID:      b.ID,
			Service: b.Name,
			Address: "127.0.0.1",
			Port:    8080,
			Tags:    []string{},
		}
		mockAgent := new(MockAgent)
		mockAgent.On("Service", b.ID, mock.Anything).Return(service, nil, nil)
		mockAgent.On("ServiceRegister", mock.Anything).Return(nil)

		mockCatalog := new(MockCatalog)
		mockCatalog.On("Service", b.Name, b.PrimaryTag, mock.Anything).Return([]*api.CatalogService{}, nil, nil)
		mockCatalog.On("Service", b.Name, "", mock.Anything).Return([]*api.CatalogService{}, nil, nil)
		mockCatalog.On("Register", mock.Anything, mock.Anything).Return(nil, nil)

		mockClient := &MockConsulClient{}
		mockClient.On("Health").Return(mockHealth)
		mockClient.On("Session").Return(mockSession)
		mockClient.On("KV").Return(mockKV)
		mockClient.On("Agent").Return(mockAgent)
		mockClient.On("Catalog").Return(mockCatalog)

		b.client = mockClient

		done := make(chan error, 1)
		go func() {
			done <- b.Run()
		}()

		// Let it run briefly then cancel
		time.Sleep(100 * time.Millisecond)
		cancel()

		err := <-done
		assert.NoError(t, err)
	})
}

func TestRun_ElectionErrorInLoop(t *testing.T) {
	t.Run("Run handles election errors in ticker loop", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		sessionID := "session_id"
		b := &Ballot{
			ID:         "test_service_id",
			Name:       "test_service",
			Key:        "election/test_service/leader",
			PrimaryTag: "primary",
			TTL:        100 * time.Millisecond,
			ctx:        ctx,
		}
		b.sessionID.Store(&sessionID)

		// Mock health to return error (triggers election error)
		mockHealth := new(MockHealth)
		electionErr := errors.New("health check failed")
		mockHealth.On("Checks", b.Name, mock.Anything).Return(nil, nil, electionErr)

		mockClient := &MockConsulClient{}
		mockClient.On("Health").Return(mockHealth)

		b.client = mockClient

		done := make(chan error, 1)
		go func() {
			done <- b.Run()
		}()

		// Let it run through at least one ticker cycle with error.
		time.Sleep(1100 * time.Millisecond)
		cancel()

		err := <-done
		assert.NoError(t, err) // Run returns nil on context cancellation, errors are logged
	})
}

func TestUpdateLeadershipStatus_Error(t *testing.T) {
	t.Run("updateLeadershipStatus returns error from updateServiceTags", func(t *testing.T) {
		mockAgent := new(MockAgent)
		expectedErr := errors.New("service tags update failed")
		mockAgent.On("Service", "test_id", mock.Anything).Return(nil, nil, expectedErr)

		mockClient := &MockConsulClient{}
		mockClient.On("Agent").Return(mockAgent)

		b := &Ballot{
			ID:     "test_id",
			client: mockClient,
		}

		err := b.updateLeadershipStatus(true)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})
}
