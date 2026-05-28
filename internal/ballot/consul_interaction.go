package ballot

import (
	"encoding/json"
	"fmt"
	"slices"

	"github.com/hashicorp/consul/api"
	log "github.com/sirupsen/logrus"
)

type ConsulElectionInteraction struct {
	client ConsulClient
	cfg    RuntimeConfig
}

func NewConsulElectionInteraction(client ConsulClient, cfg RuntimeConfig) *ConsulElectionInteraction {
	return &ConsulElectionInteraction{client: client, cfg: cfg}
}

func (i *ConsulElectionInteraction) LocalService() (*api.AgentService, []*api.CatalogService, error) {
	if i.cfg.ID == "" {
		return nil, nil, fmt.Errorf("service ID is empty; please ensure it is set in the configuration")
	}
	service, _, err := i.client.Agent().Service(i.cfg.ID, &api.QueryOptions{})
	if err != nil {
		return nil, nil, err
	}
	if service == nil {
		return nil, nil, fmt.Errorf("service %s not found", i.cfg.ID)
	}
	catalogServices, _, err := i.client.Catalog().Service(i.cfg.Name, i.cfg.PrimaryTag, &api.QueryOptions{})
	if err != nil {
		return service, nil, err
	}
	return service, catalogServices, nil
}

func (i *ConsulElectionInteraction) HealthState() (string, error) {
	healthChecks, _, err := i.client.Health().Checks(i.cfg.Name, nil)
	if err != nil {
		return "", fmt.Errorf("failed to get health checks: %w", err)
	}

	state := "passing"
	for _, check := range healthChecks {
		if check.ServiceID != i.cfg.ID {
			continue
		}
		if len(i.cfg.ServiceChecks) > 0 && !slices.Contains(i.cfg.ServiceChecks, check.CheckID) {
			continue
		}
		if check.Status == "critical" {
			return "critical", nil
		}
		if check.Status == "warning" {
			state = "warning"
		}
	}
	return state, nil
}

func (i *ConsulElectionInteraction) AcquireLeadership(payload *ElectionPayload, sessionID string) (bool, *api.WriteMeta, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return false, nil, fmt.Errorf("failed to marshal election payload: %w", err)
	}

	content := &api.KVPair{
		Key:     i.cfg.Key,
		Session: sessionID,
		Value:   data,
	}

	acquired, meta, err := i.client.KV().Acquire(content, nil)
	if err != nil {
		return false, meta, err
	}
	return acquired, meta, nil
}

func (i *ConsulElectionInteraction) LockPayload() (*ElectionPayload, error) {
	sessionKey, _, err := i.client.KV().Get(i.cfg.Key, nil)
	if err != nil {
		return nil, err
	}
	if sessionKey == nil {
		return nil, nil
	}
	var data *ElectionPayload
	if err := json.Unmarshal(sessionKey.Value, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func (i *ConsulElectionInteraction) ApplyPrimaryTag(isLeader bool) (bool, error) {
	service, _, err := i.LocalService()
	if err != nil {
		return false, err
	}

	registration := copyServiceToRegistration(service)
	if registration == nil {
		return false, fmt.Errorf("service registration is nil")
	}

	hasPrimaryTag := slices.Contains(registration.Tags, i.cfg.PrimaryTag)
	if isLeader && !hasPrimaryTag {
		registration.Tags = append(slices.Clone(registration.Tags), i.cfg.PrimaryTag)
	} else if !isLeader && hasPrimaryTag {
		index := slices.Index(registration.Tags, i.cfg.PrimaryTag)
		registration.Tags = slices.Delete(slices.Clone(registration.Tags), index, index+1)
	} else {
		return false, nil
	}

	log.WithFields(log.Fields{
		"caller":  "updateServiceTags",
		"service": i.cfg.ID,
		"tags":    registration.Tags,
	}).Debug("Updated service tags")

	return true, i.client.Agent().ServiceRegister(registration)
}

func (i *ConsulElectionInteraction) CleanupStalePrimaryTags(payload *ElectionPayload, isLeader bool) error {
	if !isLeader {
		return nil
	}

	catalogServices, _, err := i.client.Catalog().Service(i.cfg.Name, "", nil)
	if err != nil {
		return fmt.Errorf("failed to retrieve services from the catalog: %s", err)
	}

	for _, service := range catalogServices {
		if service.ServiceAddress == payload.Address && service.ServicePort == payload.Port {
			continue
		}

		primaryTagIndex := slices.Index(service.ServiceTags, i.cfg.PrimaryTag)
		if primaryTagIndex == -1 {
			continue
		}

		updatedTags := slices.Delete(slices.Clone(service.ServiceTags), primaryTagIndex, primaryTagIndex+1)
		catalogRegistration := copyCatalogServiceToRegistration(service)
		if catalogRegistration == nil || catalogRegistration.Service == nil {
			continue
		}
		catalogRegistration.Service.Tags = updatedTags

		_, err := i.client.Catalog().Register(catalogRegistration, nil)
		if err != nil {
			return fmt.Errorf("failed to update service tags in the catalog: %s", err)
		}

		log.WithFields(log.Fields{
			"caller":  "cleanup",
			"service": service.ServiceID,
			"node":    service.Node,
			"tags":    updatedTags,
		}).Info("Cleaned up primary tag from service")
	}
	return nil
}

func copyServiceToRegistration(service *api.AgentService) *api.AgentServiceRegistration {
	if service == nil {
		return nil
	}
	return &api.AgentServiceRegistration{
		ID:      service.ID,
		Name:    service.Service,
		Tags:    service.Tags,
		Port:    service.Port,
		Address: service.Address,
		Kind:    service.Kind,
		Weights: &service.Weights,
		Meta:    service.Meta,
	}
}

func copyCatalogServiceToRegistration(service *api.CatalogService) *api.CatalogRegistration {
	if service == nil {
		return nil
	}
	return &api.CatalogRegistration{
		ID:              service.ID,
		Node:            service.Node,
		Address:         service.ServiceAddress,
		TaggedAddresses: service.TaggedAddresses,
		NodeMeta:        service.NodeMeta,
		Datacenter:      service.Datacenter,
		Service: &api.AgentService{
			ID:              service.ServiceID,
			Service:         service.ServiceName,
			Tags:            service.ServiceTags,
			Meta:            service.ServiceMeta,
			Port:            service.ServicePort,
			Address:         service.ServiceAddress,
			TaggedAddresses: service.ServiceTaggedAddresses,
			Weights: api.AgentWeights{
				Passing: service.ServiceWeights.Passing,
				Warning: service.ServiceWeights.Warning,
			},
			EnableTagOverride: service.ServiceEnableTagOverride,
		},
	}
}
