package ballot

import (
	"testing"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestConsulElectionInteraction_CleanupStalePrimaryTagsSkipsServicesWithoutChanges(t *testing.T) {
	payload := &ElectionPayload{
		Address:   "127.0.0.1",
		Port:      8080,
		SessionID: "session-1",
	}
	cfg := RuntimeConfig{
		Name:       "test-service",
		PrimaryTag: "primary",
	}

	mockCatalog := new(MockCatalog)
	mockCatalog.On("Service", cfg.Name, "", (*api.QueryOptions)(nil)).Return([]*api.CatalogService{
		{
			ServiceID:      "leader",
			ServiceName:    cfg.Name,
			ServiceAddress: payload.Address,
			ServicePort:    payload.Port,
			ServiceTags:    []string{"primary"},
		},
		{
			ServiceID:      "follower",
			ServiceName:    cfg.Name,
			ServiceAddress: "127.0.0.2",
			ServicePort:    8081,
			ServiceTags:    []string{"blue"},
		},
	}, nil, nil)

	mockClient := &MockConsulClient{}
	mockClient.On("Catalog").Return(mockCatalog)

	interaction := NewConsulElectionInteraction(mockClient, cfg)
	err := interaction.CleanupStalePrimaryTags(payload, true)

	require.NoError(t, err)
	mockCatalog.AssertNotCalled(t, "Register", mock.Anything, mock.Anything)
}
