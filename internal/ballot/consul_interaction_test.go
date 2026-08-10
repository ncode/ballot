package ballot

import (
	"errors"
	"testing"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
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

// agentHealthInfo builds a local Agent health response for the configured
// service ID, mirroring the shape returned by Consul's
// AgentHealthServiceByID endpoint.
func agentHealthInfo(serviceID, serviceName, aggregated string, checks api.HealthChecks) *api.AgentServiceChecksInfo {
	return &api.AgentServiceChecksInfo{
		AggregatedStatus: aggregated,
		Service:          &api.AgentService{ID: serviceID, Service: serviceName},
		Checks:           checks,
	}
}

// TestConsulElectionInteraction_HealthState_MaintenanceClassification is a
// table-driven test covering node maintenance, configured-service maintenance,
// configured critical and warning checks, ignored ordinary checks, and passing
// health. See tasks 1.1 and 1.2.
func TestConsulElectionInteraction_HealthState_MaintenanceClassification(t *testing.T) {
	const (
		serviceID   = "test_service_id"
		serviceName = "test_service"
	)

	tests := []struct {
		name          string
		cfg           RuntimeConfig
		status        string
		info          *api.AgentServiceChecksInfo
		err           error
		wantState     string
		wantErr       bool
		wantErrSubstr string
	}{
		{
			name:      "passing health",
			cfg:       RuntimeConfig{ID: serviceID, Name: serviceName, ServiceChecks: []string{"check1"}},
			status:    api.HealthPassing,
			info:      agentHealthInfo(serviceID, serviceName, api.HealthPassing, api.HealthChecks{{ServiceID: serviceID, CheckID: "check1", Status: api.HealthPassing}}),
			wantState: "passing",
		},
		{
			name:      "warning health from configured check",
			cfg:       RuntimeConfig{ID: serviceID, Name: serviceName, ServiceChecks: []string{"check1"}},
			status:    api.HealthWarning,
			info:      agentHealthInfo(serviceID, serviceName, api.HealthWarning, api.HealthChecks{{ServiceID: serviceID, CheckID: "check1", Status: api.HealthWarning}}),
			wantState: "warning",
		},
		{
			name:      "critical health from configured check",
			cfg:       RuntimeConfig{ID: serviceID, Name: serviceName, ServiceChecks: []string{"check1"}},
			status:    api.HealthCritical,
			info:      agentHealthInfo(serviceID, serviceName, api.HealthCritical, api.HealthChecks{{ServiceID: serviceID, CheckID: "check1", Status: api.HealthCritical}}),
			wantState: "critical",
		},
		{
			name: "ignored ordinary check excluded by serviceChecks allowlist",
			cfg:  RuntimeConfig{ID: serviceID, Name: serviceName, ServiceChecks: []string{"check1"}},
			// check2 is critical but not in the allowlist; it must not block election.
			status:    api.HealthCritical,
			info:      agentHealthInfo(serviceID, serviceName, api.HealthCritical, api.HealthChecks{{ServiceID: serviceID, CheckID: "check2", Status: api.HealthCritical}}),
			wantState: "passing",
		},
		{
			name: "ordinary check for another service instance is ignored",
			cfg:  RuntimeConfig{ID: serviceID, Name: serviceName, ServiceChecks: []string{"check1"}},
			// The local Agent health response is scoped to the configured ID, but
			// a check carrying another ServiceID must still be filtered out.
			status: api.HealthCritical,
			info: agentHealthInfo(serviceID, serviceName, api.HealthCritical, api.HealthChecks{
				{ServiceID: serviceID, CheckID: "check1", Status: api.HealthPassing},
				{ServiceID: "other_service_id", CheckID: "check2", Status: api.HealthCritical},
			}),
			wantState: "passing",
		},
		{
			name:      "node maintenance via aggregated status bypasses serviceChecks",
			cfg:       RuntimeConfig{ID: serviceID, Name: serviceName, ServiceChecks: []string{"check1"}},
			status:    api.HealthCritical, // Go client maps 503 to critical
			info:      agentHealthInfo(serviceID, serviceName, api.HealthMaint, api.HealthChecks{{CheckID: api.NodeMaint, Status: api.HealthCritical}}),
			wantState: "critical",
		},
		{
			name:      "node maintenance via check identifier bypasses serviceChecks",
			cfg:       RuntimeConfig{ID: serviceID, Name: serviceName, ServiceChecks: []string{"check1"}},
			status:    api.HealthCritical,
			info:      agentHealthInfo(serviceID, serviceName, api.HealthCritical, api.HealthChecks{{CheckID: api.NodeMaint, Status: api.HealthCritical}}),
			wantState: "critical",
		},
		{
			name:      "service maintenance via check identifier bypasses serviceChecks",
			cfg:       RuntimeConfig{ID: serviceID, Name: serviceName, ServiceChecks: []string{"check1"}},
			status:    api.HealthCritical,
			info:      agentHealthInfo(serviceID, serviceName, api.HealthMaint, api.HealthChecks{{CheckID: api.ServiceMaintPrefix + serviceID, Status: api.HealthCritical}}),
			wantState: "critical",
		},
		{
			name: "service maintenance bypasses non-empty serviceChecks even when maintenance check is not allowlisted",
			cfg:  RuntimeConfig{ID: serviceID, Name: serviceName, ServiceChecks: []string{"check1"}},
			// The maintenance check id is not in serviceChecks, yet it must still
			// block election because maintenance is a control-plane signal.
			status: api.HealthCritical,
			info: agentHealthInfo(serviceID, serviceName, api.HealthMaint, api.HealthChecks{
				{ServiceID: serviceID, CheckID: api.ServiceMaintPrefix + serviceID, Status: api.HealthCritical},
				{ServiceID: serviceID, CheckID: "check1", Status: api.HealthPassing},
			}),
			wantState: "critical",
		},
		{
			name: "node maintenance takes precedence over a passing configured check",
			cfg:  RuntimeConfig{ID: serviceID, Name: serviceName, ServiceChecks: []string{"check1"}},
			status: api.HealthCritical,
			info: agentHealthInfo(serviceID, serviceName, api.HealthMaint, api.HealthChecks{
				{CheckID: api.NodeMaint, Status: api.HealthCritical},
				{ServiceID: serviceID, CheckID: "check1", Status: api.HealthPassing},
			}),
			wantState: "critical",
		},
		{
			name:          "agent health lookup error is reported",
			cfg:           RuntimeConfig{ID: serviceID, Name: serviceName, ServiceChecks: []string{"check1"}},
			status:        "",
			info:          nil,
			err:           errors.New("agent unreachable"),
			wantErr:       true,
			wantErrSubstr: "failed to get health checks",
		},
		{
			name: "missing local service stays distinguishable from maintenance",
			cfg:  RuntimeConfig{ID: serviceID, Name: serviceName, ServiceChecks: []string{"check1"}},
			// Consul reports (critical, nil, nil) when no local service matches the
			// configured ID; absence must not be treated as maintenance/critical.
			status:    api.HealthCritical,
			info:      nil,
			wantState: "passing",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockAgent := new(MockAgent)
			mockAgent.On("AgentHealthServiceByID", tc.cfg.ID).Return(tc.status, tc.info, tc.err)
			mockClient := &MockConsulClient{}
			mockClient.On("Agent").Return(mockAgent)

			interaction := NewConsulElectionInteraction(mockClient, tc.cfg)
			state, err := interaction.HealthState()

			if tc.wantErr {
				require.Error(t, err)
				if tc.wantErrSubstr != "" {
					assert.ErrorContains(t, err, tc.wantErrSubstr)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantState, state)
		})
	}
}

// TestConsulElectionInteraction_HealthState_MaintenanceBypassesAllowlist
// explicitly proves maintenance bypasses a non-empty serviceChecks allowlist
// while an ordinary critical check at the same id does not. See task 1.2.
func TestConsulElectionInteraction_HealthState_MaintenanceBypassesAllowlist(t *testing.T) {
	const (
		serviceID   = "test_service_id"
		serviceName = "test_service"
	)
	cfg := RuntimeConfig{ID: serviceID, Name: serviceName, ServiceChecks: []string{"check1"}}

	t.Run("ordinary critical check not in allowlist does not block", func(t *testing.T) {
		mockAgent := new(MockAgent)
		mockAgent.On("AgentHealthServiceByID", serviceID).Return(api.HealthCritical, agentHealthInfo(serviceID, serviceName, api.HealthCritical, api.HealthChecks{
			{ServiceID: serviceID, CheckID: "check2", Status: api.HealthCritical},
		}), nil)
		mockClient := &MockConsulClient{}
		mockClient.On("Agent").Return(mockAgent)

		state, err := NewConsulElectionInteraction(mockClient, cfg).HealthState()
		require.NoError(t, err)
		assert.Equal(t, "passing", state)
	})

	t.Run("node maintenance not in allowlist still blocks", func(t *testing.T) {
		mockAgent := new(MockAgent)
		mockAgent.On("AgentHealthServiceByID", serviceID).Return(api.HealthCritical, agentHealthInfo(serviceID, serviceName, api.HealthMaint, api.HealthChecks{
			{CheckID: api.NodeMaint, Status: api.HealthCritical},
		}), nil)
		mockClient := &MockConsulClient{}
		mockClient.On("Agent").Return(mockAgent)

		state, err := NewConsulElectionInteraction(mockClient, cfg).HealthState()
		require.NoError(t, err)
		assert.Equal(t, "critical", state)
	})

	t.Run("service maintenance not in allowlist still blocks", func(t *testing.T) {
		mockAgent := new(MockAgent)
		mockAgent.On("AgentHealthServiceByID", serviceID).Return(api.HealthCritical, agentHealthInfo(serviceID, serviceName, api.HealthMaint, api.HealthChecks{
			{ServiceID: serviceID, CheckID: api.ServiceMaintPrefix + serviceID, Status: api.HealthCritical},
		}), nil)
		mockClient := &MockConsulClient{}
		mockClient.On("Agent").Return(mockAgent)

		state, err := NewConsulElectionInteraction(mockClient, cfg).HealthState()
		require.NoError(t, err)
		assert.Equal(t, "critical", state)
	})
}

// TestConsulElectionInteraction_HealthState_LookupErrorAndMissingService
// covers Agent health lookup errors and a missing configured local service so
// absence remains distinguishable from maintenance. See task 1.3.
func TestConsulElectionInteraction_HealthState_LookupErrorAndMissingService(t *testing.T) {
	const (
		serviceID   = "test_service_id"
		serviceName = "test_service"
	)
	cfg := RuntimeConfig{ID: serviceID, Name: serviceName, ServiceChecks: []string{"check1"}}

	t.Run("agent health lookup error is wrapped and reported", func(t *testing.T) {
		lookupErr := errors.New("connection refused")
		mockAgent := new(MockAgent)
		mockAgent.On("AgentHealthServiceByID", serviceID).Return("", (*api.AgentServiceChecksInfo)(nil), lookupErr)
		mockClient := &MockConsulClient{}
		mockClient.On("Agent").Return(mockAgent)

		_, err := NewConsulElectionInteraction(mockClient, cfg).HealthState()
		require.Error(t, err)
		assert.ErrorContains(t, err, "failed to get health checks")
		assert.ErrorIs(t, err, lookupErr)
	})

	t.Run("missing configured local service reports passing not critical", func(t *testing.T) {
		mockAgent := new(MockAgent)
		mockAgent.On("AgentHealthServiceByID", serviceID).Return(api.HealthCritical, (*api.AgentServiceChecksInfo)(nil), nil)
		mockClient := &MockConsulClient{}
		mockClient.On("Agent").Return(mockAgent)

		state, err := NewConsulElectionInteraction(mockClient, cfg).HealthState()
		require.NoError(t, err)
		assert.Equal(t, "passing", state, "absence must not be converted into maintenance/critical")
	})

	t.Run("info present but service nil reports passing not critical", func(t *testing.T) {
		mockAgent := new(MockAgent)
		mockAgent.On("AgentHealthServiceByID", serviceID).Return(api.HealthCritical, &api.AgentServiceChecksInfo{
			AggregatedStatus: api.HealthCritical,
			Service:          nil,
		}, nil)
		mockClient := &MockConsulClient{}
		mockClient.On("Agent").Return(mockAgent)

		state, err := NewConsulElectionInteraction(mockClient, cfg).HealthState()
		require.NoError(t, err)
		assert.Equal(t, "passing", state)
	})
}
