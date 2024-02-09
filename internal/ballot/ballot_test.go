package ballot

import (
	"os/exec"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNew(t *testing.T) {
	b, err := New("test", "key", []string{"check1", "check2"}, "token", "promote", "demote", "primary", time.Second, time.Second)
	assert.NoError(t, err)
	assert.NotNil(t, b)
}

func TestCopyServiceToRegistration(t *testing.T) {
	b, _ := New("test", "key", []string{"check1", "check2"}, "token", "promote", "demote", "primary", time.Second, time.Second)
	service := &api.AgentService{
		ID:      "id",
		Service: "service",
		Port:    8080,
		Address: "127.0.0.1",
	}
	reg := b.copyServiceToRegistration(service)
	assert.Equal(t, service.ID, reg.ID)
	assert.Equal(t, service.Service, reg.Name)
	assert.Equal(t, service.Port, reg.Port)
	assert.Equal(t, service.Address, reg.Address)
}

func TestCopyCatalogServiceToRegistration(t *testing.T) {
	b, _ := New("test", "key", []string{"check1", "check2"}, "token", "promote", "demote", "primary", time.Second, time.Second)
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
	reg := b.copyCatalogServiceToRegistration(service)
	assert.Equal(t, service.ID, reg.ID)
	assert.Equal(t, service.Node, reg.Node)
	assert.Equal(t, service.ServiceAddress, reg.Address)
	assert.Equal(t, service.ServiceID, reg.Service.ID)
	assert.Equal(t, service.ServiceName, reg.Service.Service)
	assert.Equal(t, service.ServicePort, reg.Service.Port)
	assert.Equal(t, service.ServiceTags, reg.Service.Tags)
	assert.Equal(t, service.ServiceMeta, reg.Service.Meta)
	assert.Equal(t, service.ServiceWeights.Passing, reg.Service.Weights.Passing)
	assert.Equal(t, service.ServiceWeights.Warning, reg.Service.Weights.Warning)
	assert.Equal(t, service.ServiceEnableTagOverride, reg.Service.EnableTagOverride)
}

// MockCommandExecutor is a mock implementation of the CommandExecutor interface
type MockCommandExecutor struct {
	mock.Mock
}

func (m *MockCommandExecutor) Command(name string, arg ...string) *exec.Cmd {
	args := m.Called(name, arg)
	return args.Get(0).(*exec.Cmd)
}

func TestRunCommand(t *testing.T) {
	// Create a mock CommandExecutor
	mockExecutor := new(MockCommandExecutor)

	// Create a Ballot instance with the mock executor
	b := &Ballot{
		exec: mockExecutor,
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
	mockExecutor.On("Command", "echo", []string{"hello"}).Return(mockCmd)

	// Call the method under test
	_, err := b.runCommand(command, payload)

	// Assert that the expectations were met
	mockExecutor.AssertExpectations(t)

	// Assert that the method did not return an error
	assert.NoError(t, err)
}
