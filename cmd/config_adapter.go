package cmd

import (
	"github.com/ncode/ballot/internal/ballot"
	"github.com/spf13/viper"
)

func runtimeConfigFromViper(name string) (ballot.RuntimeConfig, error) {
	serviceKey := "election.services." + name + "."
	config := ballot.LoadedConfig{
		Consul: ballot.LoadedConsulConfig{
			Address: viper.GetString("consul.address"),
			Token:   viper.GetString("consul.token"),
		},
		Election: ballot.LoadedElectionConfig{
			Services: map[string]ballot.LoadedServiceConfig{
				name: {
					Name:          viper.GetString(serviceKey + "name"),
					ID:            viper.GetString(serviceKey + "id"),
					Key:           viper.GetString(serviceKey + "key"),
					ServiceChecks: viper.GetStringSlice(serviceKey + "serviceChecks"),
					Token:         viper.GetString(serviceKey + "token"),
					ExecOnPromote: viper.GetString(serviceKey + "execOnPromote"),
					ExecOnDemote:  viper.GetString(serviceKey + "execOnDemote"),
					PrimaryTag:    viper.GetString(serviceKey + "primaryTag"),
					TTL:           viper.GetString(serviceKey + "ttl"),
					LockDelay:     viper.GetString(serviceKey + "lockDelay"),
				},
			},
		},
	}
	return ballot.RuntimeConfigFor(config, name)
}
