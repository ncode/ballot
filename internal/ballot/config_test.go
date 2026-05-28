package ballot

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRuntimeConfigFor(t *testing.T) {
	t.Run("named service", func(t *testing.T) {
		cfg, err := RuntimeConfigFor(LoadedConfig{
			Consul: LoadedConsulConfig{
				Address: "127.0.0.1:8500",
				Token:   "global-token",
			},
			Election: LoadedElectionConfig{
				Services: map[string]LoadedServiceConfig{
					"my-service": {
						Name:          "service-name",
						ID:            "service-id",
						Key:           "election/service/leader",
						ServiceChecks: []string{"service:service-id"},
						Token:         "service-token",
						PrimaryTag:    "primary",
						ExecOnPromote: "echo promoted",
						ExecOnDemote:  "echo demoted",
						TTL:           "15s",
						LockDelay:     "5s",
					},
				},
			},
		}, "my-service")

		require.NoError(t, err)
		assert.Equal(t, "service-name", cfg.Name)
		assert.Equal(t, "service-id", cfg.ID)
		assert.Equal(t, "election/service/leader", cfg.Key)
		assert.Equal(t, []string{"service:service-id"}, cfg.ServiceChecks)
		assert.Equal(t, "service-token", cfg.ConsulToken)
		assert.Equal(t, "127.0.0.1:8500", cfg.ConsulAddress)
		assert.Equal(t, "primary", cfg.PrimaryTag)
		assert.Equal(t, "echo promoted", cfg.ExecOnPromote)
		assert.Equal(t, "echo demoted", cfg.ExecOnDemote)
		assert.Equal(t, 15*time.Second, cfg.TTL)
		assert.Equal(t, 5*time.Second, cfg.LockDelay)
	})

	t.Run("defaults", func(t *testing.T) {
		cfg, err := RuntimeConfigFor(LoadedConfig{
			Election: LoadedElectionConfig{
				Services: map[string]LoadedServiceConfig{
					"my-service": {
						ID:  "service-id",
						Key: "election/service/leader",
					},
				},
			},
		}, "my-service")

		require.NoError(t, err)
		assert.Equal(t, "my-service", cfg.Name)
		assert.Equal(t, DefaultSessionTTL, cfg.TTL)
		assert.Equal(t, DefaultLockDelay, cfg.LockDelay)
	})

	t.Run("missing service ID", func(t *testing.T) {
		_, err := RuntimeConfigFor(LoadedConfig{
			Election: LoadedElectionConfig{
				Services: map[string]LoadedServiceConfig{
					"my-service": {Key: "election/service/leader"},
				},
			},
		}, "my-service")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "service ID is required")
	})

	t.Run("missing key", func(t *testing.T) {
		_, err := RuntimeConfigFor(LoadedConfig{
			Election: LoadedElectionConfig{
				Services: map[string]LoadedServiceConfig{
					"my-service": {ID: "service-id"},
				},
			},
		}, "my-service")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "key is required")
	})

	t.Run("malformed duration", func(t *testing.T) {
		_, err := RuntimeConfigFor(LoadedConfig{
			Election: LoadedElectionConfig{
				Services: map[string]LoadedServiceConfig{
					"my-service": {
						ID:  "service-id",
						Key: "election/service/leader",
						TTL: "definitely-not-a-duration",
					},
				},
			},
		}, "my-service")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "ttl")
	})
}
