package ballot

import (
	"context"
	"fmt"
	"os/exec"
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
