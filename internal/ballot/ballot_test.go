package ballot

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"slices"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNew(t *testing.T) {
	t.Run("successful new", func(t *testing.T) {
		// Set up the necessary configuration
		viper.Set("election.services.test.id", "test_service_id")
		viper.Set("election.services.test.key", "election/test_service/leader")
		viper.Set("election.services.test.primaryTag", "primary")
		viper.Set("election.services.test.serviceChecks", []string{"service:test_service_id"})
		viper.Set("election.services.test.execOnPromote", "echo Promoted to leader")
		viper.Set("election.services.test.execOnDemote", "echo Demoted from leader")
		viper.Set("election.services.test.ttl", "10s")
		viper.Set("election.services.test.lockDelay", "3s")

		// Ensure viper configuration is reset after the test
		defer func() {
			viper.Reset()
		}()

		// Call the New function
		b, err := New(context.Background(), "test")
		assert.NoError(t, err)
		assert.NotNil(t, b)

		// Verify that the Ballot instance has the expected values
		assert.Equal(t, "test_service_id", b.ID)
		assert.Equal(t, "election/test_service/leader", b.Key)
		assert.Equal(t, "primary", b.PrimaryTag)
	})

	t.Run("failure due to nil context", func(t *testing.T) {
		b, err := New(nil, "test")
		assert.Error(t, err)
		assert.Nil(t, b)
	})
}

func TestCopyServiceToRegistration(t *testing.T) {
	b := &Ballot{}

	t.Run("successful copy", func(t *testing.T) {
		service := &api.AgentService{
			ID:      "testID",
			Service: "testService",
			Tags:    []string{"tag1", "tag2"},
			Port:    8080,
			Address: "127.0.0.1",
		}

		registration := b.copyServiceToRegistration(service)

		assert.Equal(t, service.ID, registration.ID)
		assert.Equal(t, service.Service, registration.Name)
		assert.Equal(t, service.Tags, registration.Tags)
		assert.Equal(t, service.Port, registration.Port)
		assert.Equal(t, service.Address, registration.Address)
	})

	t.Run("handles nil service gracefully", func(t *testing.T) {
		registration := b.copyServiceToRegistration(nil)
		assert.Nil(t, registration)
	})
}

func TestCopyCatalogServiceToRegistration(t *testing.T) {
	b := &Ballot{}

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

		registration := b.copyCatalogServiceToRegistration(service)
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
		registration := b.copyCatalogServiceToRegistration(nil)
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

func TestRunCommand(t *testing.T) {
	// Create a mock CommandExecutor
	mockExecutor := new(MockCommandExecutor)

	// Create a Ballot instance with the mock executor
	b := &Ballot{
		executor: mockExecutor,
		ctx:      context.Background(),
	}

	// Define the command to run
	command := "echo hello"
	payload := &ElectionPayload{
		Address:   "127.0.0.1",
		Port:      8080,
		SessionID: "session",
	}

	// Set up the expectation
	// Here, we're using a command that just outputs "mocked" when run
	mockCmd := exec.Command("echo", "mocked")
	mockExecutor.On("CommandContext", b.ctx, "echo", []string{"hello"}).Return(mockCmd)

	// Call the method under test
	_, err := b.runCommand(command, payload)

	// Assert that the expectations were met
	mockExecutor.AssertExpectations(t)

	// Assert that the method did not return an error
	assert.NoError(t, err)
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

	// Subtest: Successful cleanup when leader
	t.Run("Successful_cleanup_when_leader", func(t *testing.T) {
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
			ExecOnPromote: "", // Prevent running commands
			ExecOnDemote:  "", // Prevent running commands
			executor:      &commandExecutor{},
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
			ExecOnPromote: "", // Prevent running commands
			ExecOnDemote:  "", // Prevent running commands
			executor:      &commandExecutor{},
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
		// Create fresh mocks for this subtest
		mockCatalog := new(MockCatalog)
		mockClient := &MockConsulClient{}
		mockClient.On("Catalog").Return(mockCatalog)

		// Mock the Catalog().Service call to return the other service with the primary tag
		mockCatalog.On("Service", serviceName, "", mock.AnythingOfType("*api.QueryOptions")).Return([]*api.CatalogService{otherService}, nil, nil)

		// Mock the Catalog().Register to return an error
		expectedErr := fmt.Errorf("failed to register catalog service")
		mockCatalog.On("Register", mock.MatchedBy(func(reg *api.CatalogRegistration) bool {
			if reg.Service == nil {
				return false
			}
			return !slices.Contains(reg.Service.Tags, primaryTag)
		}), mock.Anything).Return(nil, expectedErr)

		// Initialize the Ballot instance with the mock client
		b := &Ballot{
			client:        mockClient,
			Name:          serviceName,
			PrimaryTag:    primaryTag,
			ctx:           context.Background(),
			ExecOnPromote: "", // Prevent running commands
			ExecOnDemote:  "", // Prevent running commands
			executor:      &commandExecutor{},
		}

		// Set the Ballot as leader and set sessionID
		sessionID := leaderPayload.SessionID
		b.leader.Store(true)
		b.sessionID.Store(&sessionID)

		// Execute the method under test
		err := b.cleanup(leaderPayload)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)

		// Assert that Catalog().Register was called once with the primary tag removed
		mockCatalog.AssertCalled(t, "Register", mock.MatchedBy(func(reg *api.CatalogRegistration) bool {
			if reg.Service == nil {
				return false
			}
			return !slices.Contains(reg.Service.Tags, primaryTag)
		}), mock.Anything)
	})
}

func TestAttemptLeadershipAcquisition(t *testing.T) {
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

	mockKV := new(MockKV)
	mockKV.On("Acquire", mock.Anything, (*api.WriteOptions)(nil)).Return(true, &api.WriteMeta{}, nil)

	mockClient := &MockConsulClient{}
	mockClient.On("KV").Return(mockKV)

	b.client = mockClient

	t.Run("Successful acquisition", func(t *testing.T) {
		acquired, _, err := b.attemptLeadershipAcquisition(payload)
		assert.NoError(t, err)
		assert.True(t, acquired)
	})

	t.Run("Failure due to nil session ID", func(t *testing.T) {
		b.sessionID.Store((*string)(nil))
		acquired, _, err := b.attemptLeadershipAcquisition(payload)
		assert.Error(t, err)
		assert.False(t, acquired)
	})

	t.Run("Failure due to KV Acquire error", func(t *testing.T) {
		b.sessionID.Store(&sessionID)
		expectedErr := fmt.Errorf("KV acquire error")
		mockKV.On("Acquire", mock.Anything, (*api.WriteOptions)(nil)).Return(false, nil, expectedErr)

		acquired, _, err := b.attemptLeadershipAcquisition(payload)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.False(t, acquired)
	})
}

func TestVerifyAndUpdateLeadershipStatus(t *testing.T) {
	sessionID := "session_id"
	b := &Ballot{}
	b.sessionID.Store(&sessionID)

	payload := &ElectionPayload{
		SessionID: sessionID,
	}

	mockKV := new(MockKV)
	data, _ := json.Marshal(payload)
	mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(&api.KVPair{
		Key:     b.Key,
		Value:   data,
		Session: sessionID,
	}, nil, nil)

	mockClient := &MockConsulClient{}
	mockClient.On("KV").Return(mockKV)

	b.client = mockClient

	t.Run("Instance is leader", func(t *testing.T) {
		err := b.verifyAndUpdateLeadershipStatus()
		assert.NoError(t, err)
		assert.True(t, b.IsLeader())
	})

	t.Run("Instance is not leader", func(t *testing.T) {
		otherSessionID := "other_session_id"
		payload.SessionID = otherSessionID
		data, _ = json.Marshal(payload)
		mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(&api.KVPair{
			Key:     b.Key,
			Value:   data,
			Session: otherSessionID,
		}, nil, nil)

		err := b.verifyAndUpdateLeadershipStatus()
		assert.NoError(t, err)
		assert.False(t, b.IsLeader())
	})

	t.Run("Error getting session data", func(t *testing.T) {
		expectedErr := fmt.Errorf("KV get error")
		mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(nil, nil, expectedErr)

		err := b.verifyAndUpdateLeadershipStatus()
		assert.Error(t, err)
		assert.ErrorContains(t, err, expectedErr.Error())
	})
}

func TestWaitForNextValidSessionData(t *testing.T) {
	b := &Ballot{}
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

	t.Run("Successfully retrieves session data", func(t *testing.T) {
		data, err := b.waitForNextValidSessionData(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, data)
		assert.Equal(t, payload.SessionID, data.SessionID)
	})

	t.Run("Handles context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
		defer cancel()
		data, err := b.waitForNextValidSessionData(ctx)
		assert.Error(t, err)
		assert.Nil(t, data)
	})

	t.Run("Handles errors when getting session data", func(t *testing.T) {
		expectedErr := fmt.Errorf("KV get error")
		mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(nil, nil, expectedErr)

		data, err := b.waitForNextValidSessionData(ctx)
		assert.Error(t, err)
		assert.ErrorContains(t, err, expectedErr.Error())
		assert.Nil(t, data)
	})
}

func TestGetSessionData(t *testing.T) {
	b := &Ballot{
		Key: "election/test_service/leader",
	}

	mockKV := new(MockKV)
	mockClient := &MockConsulClient{}
	mockClient.On("KV").Return(mockKV)
	b.client = mockClient

	t.Run("Successfully retrieves session data", func(t *testing.T) {
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
		mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(nil, nil, nil)

		result, err := b.getSessionData()
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("Error from KV store", func(t *testing.T) {
		expectedErr := fmt.Errorf("KV get error")
		mockKV.On("Get", b.Key, (*api.QueryOptions)(nil)).Return(nil, nil, expectedErr)

		result, err := b.getSessionData()
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("JSON unmarshal error", func(t *testing.T) {
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
	mockHealth.On("Checks", b.Name, (*api.QueryOptions)(nil)).Return([]*api.HealthCheck{
		{Status: "passing"},
	}, nil, nil)

	// Mock session
	sessionID := "session_id"
	mockSession := new(MockSession)
	mockSession.On("Create", mock.Anything, (*api.WriteOptions)(nil)).Return(sessionID, nil, nil)
	mockSession.On("RenewPeriodic", mock.Anything, sessionID, (*api.WriteOptions)(nil), mock.Anything).Return(nil)
	mockSession.On("Info", sessionID, (*api.QueryOptions)(nil)).Return(&api.SessionEntry{ID: sessionID}, nil, nil)

	// Mock KV
	mockKV := new(MockKV)
	mockKV.On("Acquire", mock.Anything, (*api.WriteOptions)(nil)).Return(true, nil, nil)

	// Mock Agent
	service := &api.AgentService{
		ID:      b.ID,
		Service: b.Name,
		Address: "127.0.0.1",
		Port:    8080,
	}
	mockAgent := new(MockAgent)
	mockAgent.On("Service", b.ID, (*api.QueryOptions)(nil)).Return(service, nil, nil)

	// Mock Catalog
	mockCatalog := new(MockCatalog)
	mockCatalog.On("Service", b.Name, b.PrimaryTag, (*api.QueryOptions)(nil)).Return([]*api.CatalogService{}, nil, nil)

	// Set up the mock client
	mockClient := &MockConsulClient{}
	mockClient.On("Health").Return(mockHealth)
	mockClient.On("Session").Return(mockSession)
	mockClient.On("KV").Return(mockKV)
	mockClient.On("Agent").Return(mockAgent)
	mockClient.On("Catalog").Return(mockCatalog)

	b.client = mockClient

	t.Run("Successful election", func(t *testing.T) {
		err := b.election()
		assert.NoError(t, err)
		assert.True(t, b.IsLeader())
	})

	t.Run("Service in critical state", func(t *testing.T) {
		mockHealth.On("Checks", b.Name, (*api.QueryOptions)(nil)).Return([]*api.HealthCheck{
			{Status: "critical"},
		}, nil, nil)

		err := b.election()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "service is in critical state")
	})

	t.Run("Session creation fails", func(t *testing.T) {
		expectedErr := fmt.Errorf("session creation error")
		mockSession.On("Create", mock.Anything, (*api.WriteOptions)(nil)).Return("", nil, expectedErr)

		err := b.election()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), expectedErr.Error())
	})
}

// MockConsulClient is a mock implementation of the api.Client interface
type MockConsulClient struct {
	mock.Mock
}

func (m *MockConsulClient) Agent() AgentInterface {
	args := m.Called()
	return args.Get(0).(AgentInterface)
}

func (m *MockConsulClient) Catalog() CatalogInterface {
	args := m.Called()
	return args.Get(0).(CatalogInterface)
}

func (m *MockConsulClient) Health() HealthInterface {
	args := m.Called()
	return args.Get(0).(HealthInterface)
}

func (m *MockConsulClient) KV() KVInterface {
	args := m.Called()
	return args.Get(0).(KVInterface)
}

func (m *MockConsulClient) Session() SessionInterface {
	args := m.Called()
	return args.Get(0).(SessionInterface)
}

// MockAgent is a mock implementation of the api.Agent interface
type MockAgent struct {
	mock.Mock
}

func (m *MockAgent) Service(serviceID string, q *api.QueryOptions) (*api.AgentService, *api.QueryMeta, error) {
	args := m.Called(serviceID, q)
	var service *api.AgentService
	if args.Get(0) != nil {
		service = args.Get(0).(*api.AgentService)
	}
	var meta *api.QueryMeta
	if args.Get(1) != nil {
		meta = args.Get(1).(*api.QueryMeta)
	}
	return service, meta, args.Error(2)
}

func (m *MockAgent) ServiceRegister(service *api.AgentServiceRegistration) error {
	args := m.Called(service)
	return args.Error(0)
}

func (m *MockAgent) ServiceDeregister(serviceID string) error {
	args := m.Called(serviceID)
	return args.Error(0)
}

// MockCatalog is a mock implementation of the api.Catalog interface
type MockCatalog struct {
	mock.Mock
}

func (m *MockCatalog) Service(serviceName, tag string, q *api.QueryOptions) ([]*api.CatalogService, *api.QueryMeta, error) {
	args := m.Called(serviceName, tag, q)
	var services []*api.CatalogService
	if args.Get(0) != nil {
		services = args.Get(0).([]*api.CatalogService)
	}
	var meta *api.QueryMeta
	if args.Get(1) != nil {
		meta = args.Get(1).(*api.QueryMeta)
	}
	return services, meta, args.Error(2)
}

func (m *MockCatalog) Register(reg *api.CatalogRegistration, w *api.WriteOptions) (*api.WriteMeta, error) {
	args := m.Called(reg, w)
	var meta *api.WriteMeta
	if args.Get(0) != nil {
		meta = args.Get(0).(*api.WriteMeta)
	}
	return meta, args.Error(1)
}

func (m *MockCatalog) Deregister(dereg *api.CatalogDeregistration, w *api.WriteOptions) (*api.WriteMeta, error) {
	args := m.Called(dereg, w)
	var meta *api.WriteMeta
	if args.Get(0) != nil {
		meta = args.Get(0).(*api.WriteMeta)
	}
	return meta, args.Error(1)
}

// MockSession is a mock implementation of the api.Session interface
type MockSession struct {
	mock.Mock
}

func (m *MockSession) Create(se *api.SessionEntry, q *api.WriteOptions) (string, *api.WriteMeta, error) {
	args := m.Called(se, q)
	sessionID := args.String(0)
	var meta *api.WriteMeta
	if args.Get(1) != nil {
		meta = args.Get(1).(*api.WriteMeta)
	}
	return sessionID, meta, args.Error(2)
}

func (m *MockSession) Destroy(sessionID string, q *api.WriteOptions) (*api.WriteMeta, error) {
	args := m.Called(sessionID, q)
	var meta *api.WriteMeta
	if args.Get(0) != nil {
		meta = args.Get(0).(*api.WriteMeta)
	}
	return meta, args.Error(1)
}

func (m *MockSession) Info(sessionID string, q *api.QueryOptions) (*api.SessionEntry, *api.QueryMeta, error) {
	args := m.Called(sessionID, q)
	return args.Get(0).(*api.SessionEntry), args.Get(1).(*api.QueryMeta), args.Error(2)
}

func (m *MockSession) RenewPeriodic(initialTTL string, sessionID string, q *api.WriteOptions, doneCh <-chan struct{}) error {
	args := m.Called(initialTTL, sessionID, q, doneCh)
	return args.Error(0)
}

// MockHealth is a mock implementation of the api.Health interface
type MockHealth struct {
	mock.Mock
}

func (m *MockHealth) Checks(service string, q *api.QueryOptions) ([]*api.HealthCheck, *api.QueryMeta, error) {
	args := m.Called(service, q)
	checks, _ := args.Get(0).([]*api.HealthCheck)
	meta, _ := args.Get(1).(*api.QueryMeta)
	return checks, meta, args.Error(2)
}

// MockKV is a mock implementation of the api.KV interface
type MockKV struct {
	mock.Mock
}

func (m *MockKV) Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error) {
	args := m.Called(key, q)
	pair, _ := args.Get(0).(*api.KVPair)
	meta, _ := args.Get(1).(*api.QueryMeta)
	return pair, meta, args.Error(2)
}

func (m *MockKV) Put(p *api.KVPair, q *api.WriteOptions) (*api.WriteMeta, error) {
	args := m.Called(p, q)
	meta, _ := args.Get(0).(*api.WriteMeta)
	return meta, args.Error(1)
}

func (m *MockKV) Acquire(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error) {
	args := m.Called(p, q)
	return args.Bool(0), args.Get(1).(*api.WriteMeta), args.Error(2)
}
