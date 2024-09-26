package ballot

import (
	"context"
	"os/exec"
	"testing"

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

// MockConsulClient is a mock implementation of the ConsulClient interface
type MockConsulClient struct {
	mock.Mock
}

func (m *MockConsulClient) Agent() *api.Agent {
	args := m.Called()
	return args.Get(0).(*api.Agent)
}

func (m *MockConsulClient) Catalog() *api.Catalog {
	args := m.Called()
	return args.Get(0).(*api.Catalog)
}

func (m *MockConsulClient) KV() *api.KV {
	args := m.Called()
	return args.Get(0).(*api.KV)
}

func (m *MockConsulClient) Session() *api.Session {
	args := m.Called()
	return args.Get(0).(*api.Session)
}

func (m *MockConsulClient) Health() *api.Health {
	args := m.Called()
	return args.Get(0).(*api.Health)
}
