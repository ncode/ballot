package ballot

import (
	"fmt"
	"time"
)

const (
	DefaultSessionTTL = 10 * time.Second
	DefaultLockDelay  = 3 * time.Second
)

type LoadedConfig struct {
	Consul   LoadedConsulConfig   `mapstructure:"consul"`
	Election LoadedElectionConfig `mapstructure:"election"`
}

type LoadedConsulConfig struct {
	Address string `mapstructure:"address"`
	Token   string `mapstructure:"token"`
}

type LoadedElectionConfig struct {
	Enabled  []string                       `mapstructure:"enabled"`
	Services map[string]LoadedServiceConfig `mapstructure:"services"`
}

type LoadedServiceConfig struct {
	Name          string   `mapstructure:"name"`
	ID            string   `mapstructure:"id"`
	Key           string   `mapstructure:"key"`
	ServiceChecks []string `mapstructure:"serviceChecks"`
	Token         string   `mapstructure:"token"`
	ExecOnPromote string   `mapstructure:"execOnPromote"`
	ExecOnDemote  string   `mapstructure:"execOnDemote"`
	PrimaryTag    string   `mapstructure:"primaryTag"`
	TTL           string   `mapstructure:"ttl"`
	LockDelay     string   `mapstructure:"lockDelay"`
}

type RuntimeConfig struct {
	Name          string
	ID            string
	Key           string
	ServiceChecks []string
	ConsulToken   string
	ExecOnPromote string
	ExecOnDemote  string
	PrimaryTag    string
	ConsulAddress string
	TTL           time.Duration
	LockDelay     time.Duration
}

func RuntimeConfigFor(config LoadedConfig, name string) (RuntimeConfig, error) {
	services := config.Election.Services
	if services == nil {
		return RuntimeConfig{}, fmt.Errorf("service %q is not configured", name)
	}

	service, ok := services[name]
	if !ok {
		return RuntimeConfig{}, fmt.Errorf("service %q is not configured", name)
	}

	cfg := RuntimeConfig{
		Name:          service.Name,
		ID:            service.ID,
		Key:           service.Key,
		ServiceChecks: service.ServiceChecks,
		ConsulToken:   config.Consul.Token,
		ExecOnPromote: service.ExecOnPromote,
		ExecOnDemote:  service.ExecOnDemote,
		PrimaryTag:    service.PrimaryTag,
		ConsulAddress: config.Consul.Address,
		TTL:           DefaultSessionTTL,
		LockDelay:     DefaultLockDelay,
	}
	if cfg.Name == "" {
		cfg.Name = name
	}
	if service.Token != "" {
		cfg.ConsulToken = service.Token
	}

	var err error
	if service.TTL != "" {
		cfg.TTL, err = time.ParseDuration(service.TTL)
		if err != nil {
			return RuntimeConfig{}, fmt.Errorf("ttl for service %q is invalid: %w", name, err)
		}
	}
	if service.LockDelay != "" {
		cfg.LockDelay, err = time.ParseDuration(service.LockDelay)
		if err != nil {
			return RuntimeConfig{}, fmt.Errorf("lockDelay for service %q is invalid: %w", name, err)
		}
	}

	if err := validateRuntimeConfig(cfg); err != nil {
		return RuntimeConfig{}, err
	}
	return cfg, nil
}

func validateRuntimeConfig(cfg RuntimeConfig) error {
	if cfg.ID == "" {
		return fmt.Errorf("service ID is required; please set the 'id' field in the configuration")
	}
	if cfg.Key == "" {
		return fmt.Errorf("key is required; please set the 'key' field in the configuration")
	}
	if cfg.Name == "" {
		return fmt.Errorf("service name is required")
	}
	if cfg.TTL == 0 {
		return fmt.Errorf("ttl is required")
	}
	if cfg.LockDelay == 0 {
		return fmt.Errorf("lockDelay is required")
	}
	return nil
}
