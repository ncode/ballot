package ballot

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"sync/atomic"
	"time"

	"github.com/google/shlex"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
	"golang.org/x/net/context"
)

type ElectionPayload struct {
	Address   string
	Port      int
	SessionID string
}

// New returns a new Ballot instance.
func New(name string, key string, serviceChecks []string, token string, execOnPromote string, execOnDemote string, primaryTag string, ttl time.Duration, lockDelay time.Duration) (b *Ballot, err error) {
	b = &Ballot{
		Name:          name,
		Key:           key,
		ServiceChecks: serviceChecks,
		Token:         token,
		ExecOnPromote: execOnPromote,
		ExecOnDemote:  execOnDemote,
		PrimaryTag:    primaryTag,
		TTL:           ttl,
		LockDelay:     lockDelay,
		client:        nil,
	}

	return b, err
}

// Ballot is a struct that holds the configuration for the leader election.
type Ballot struct {
	Name          string          `mapstructure:"-"`
	ID            string          `mapstructure:"id"`
	Key           string          `mapstructure:"key"`
	ServiceChecks []string        `mapstructure:"serviceChecks"`
	Token         string          `mapstructure:"token"`
	ExecOnPromote string          `mapstructure:"execOnPromote"`
	ExecOnDemote  string          `mapstructure:"execOnDemote"`
	PrimaryTag    string          `mapstructure:"primaryTag"`
	TTL           time.Duration   `mapstructure:"ttl"`
	LockDelay     time.Duration   `mapstructure:"lockDelay"`
	sessionID     atomic.Value    `mapstructure:"-"`
	leader        atomic.Bool     `mapstructure:"-"`
	client        *api.Client     `mapstructure:"-"`
	ctx           context.Context `mapstructure:"-"`
}

// runCommand runs a command and returns the output.
func (b *Ballot) runCommand(command string, electionPayload *ElectionPayload) ([]byte, error) {
	log.WithFields(log.Fields{
		"caller": "runCommand",
	}).Info("Running command: ", command)
	args, err := shlex.Split(command)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Env = append(cmd.Env, fmt.Sprintf("ADDRESS=%s", electionPayload.Address))
	cmd.Env = append(cmd.Env, fmt.Sprintf("PORT=%d", electionPayload.Port))
	cmd.Env = append(cmd.Env, fmt.Sprintf("SESSIONID=%s", electionPayload.SessionID))
	return cmd.Output()
}

// Copy *api.AgentService to *api.AgentServiceRegistration
func (b *Ballot) copyServiceToRegistration(service *api.AgentService) *api.AgentServiceRegistration {
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

func (b *Ballot) copyCatalogServiceToRegistration(service *api.CatalogService) *api.CatalogRegistration {
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

// Run starts the leader election.
func (b *Ballot) Run() (err error) {
	b.ctx = context.Background()
	consulConfig := api.DefaultConfig()
	consulConfig.Token = b.Token
	client, err := api.NewClient(consulConfig)
	if err != nil {
		return err
	}
	b.client = client
	b.electionLoop()

	return nil
}

// getService returns the registered service.
func (b *Ballot) getService() (service *api.AgentService, catalogServices []*api.CatalogService, err error) {
	agent := b.client.Agent()
	service, _, err = agent.Service(b.ID, &api.QueryOptions{})
	if err != nil {
		return service, nil, err
	}
	if service == nil {
		return service, nil, fmt.Errorf("service %s not found", b.ID)
	}
	catalog := b.client.Catalog()
	catalogServices, _, err = catalog.Service(b.ID, b.PrimaryTag, &api.QueryOptions{})
	if err != nil {
		return service, nil, err
	}
	return service, catalogServices, err
}

// updateServiceTags updates the service tags.
func (b *Ballot) updateServiceTags() error {
	service, _, err := b.getService()
	if err != nil {
		return err
	}

	// Get a copy of the current service registration
	registration := b.copyServiceToRegistration(service)

	// Determine if the primary tag is already present
	hasPrimaryTag := slices.Contains(registration.Tags, b.PrimaryTag)

	// Update tags based on leadership status
	if b.IsLeader() && !hasPrimaryTag {
		// Add primary tag if not present and this node is the leader
		registration.Tags = append(registration.Tags, b.PrimaryTag)
	} else if !b.IsLeader() && hasPrimaryTag {
		// Remove primary tag if present and this node is not the leader
		index := slices.Index(registration.Tags, b.PrimaryTag)
		registration.Tags = append(registration.Tags[:index], registration.Tags[index+1:]...)
	} else {
		// No changes needed
		return nil
	}

	// Log the updated tags
	log.WithFields(log.Fields{
		"caller":  "updateServiceTags",
		"service": b.ID,
		"tags":    registration.Tags,
	}).Debug("Updated service tags")

	// Update the service registration with new tags
	agent := b.client.Agent()
	return agent.ServiceRegister(registration)
}

// cleanup is called on promote, it cleans up tags on the instances of the service, useful if an other ballot stopped unexpectedly
func (b *Ballot) cleanup() error {
	service, catalogServices, err := b.getService()
	if err != nil {
		return err
	}
	for _, catcatalogService := range catalogServices {
		if catcatalogService.Address != service.Address {
			p := slices.Index(catcatalogService.ServiceTags, b.PrimaryTag)
			if p == -1 {
				return nil
			}
			log.WithFields(log.Fields{
				"caller":  "cleanupCatalogServiceTags",
				"service": b.ID,
				"node":    catcatalogService.Node,
				"tags":    catcatalogService.ServiceTags,
			}).Debug("current service tags")
			catcatalogService.ServiceTags = slices.Delete(catcatalogService.ServiceTags, p, p+1)
			catalog := b.client.Catalog()
			reg := b.copyCatalogServiceToRegistration(catcatalogService)
			_, err := catalog.Register(reg, &api.WriteOptions{})
			if err != nil {
				return err
			}
			log.WithFields(log.Fields{
				"caller":  "cleanupCatalogServiceTags",
				"service": b.ID,
				"node":    catcatalogService.Node,
				"tags":    catcatalogService.ServiceTags,
			}).Debug("new service tags")
		}
	}
	return nil
}

// onPromote is called when the node is promoted to leader.
func (b *Ballot) onPromote() (err error) {
	b.leader.Store(true)
	err = b.updateServiceTags()
	if err != nil {
		b.releaseSession()
		return fmt.Errorf("failed to update service tags: %s", err)
	}
	err = b.cleanup()
	if err != nil {
		b.releaseSession()
		return fmt.Errorf("failed to cleanup old service tags: %s", err)
	}
	return err
}

// election is responsible for the leader election.
func (b *Ballot) election() (err error) {
	err = b.session()
	if err != nil {
		fmt.Println("here Juliano")
		if b.leader.Load() {
			err := b.onDemote()
			if err != nil {
				return fmt.Errorf("failed to validate session as leader, forcing demotion: %s", err.Error())
			}
		}
		return fmt.Errorf("failed to create session: %s", err)
	}

	if !b.leader.Load() {
		if b.sessionID.Load() != nil {
			service, _, err := b.getService()
			if err != nil {
				return fmt.Errorf("failed to get service: %s", err)
			}

			electionPayload := &ElectionPayload{
				Address:   service.Address,
				Port:      service.Port,
				SessionID: b.sessionID.Load().(string),
			}

			payload, err := json.Marshal(electionPayload)
			if err != nil {
				return fmt.Errorf("failed to marshal election payload: %s", err)
			}

			content := &api.KVPair{
				Key:     b.Key,
				Session: b.sessionID.Load().(string),
				Value:   payload,
			}

			_, _, err = b.client.KV().Acquire(content, nil)
			if err != nil {
				return fmt.Errorf("failed to acquire lock: %s", err)
			}
		}
	}

	session, err := b.getSessionKey()
	if err == nil {
		if b.sessionID.Load() != nil && session == b.sessionID.Load().(string) {
			if !b.IsLeader() {
				err = b.onPromote()
				if err != nil {
					return fmt.Errorf("failures during promotion: %s", err.Error())
				}
			}
		} else {
			if b.IsLeader() {
				err = b.onDemote()
				if err != nil {
					return fmt.Errorf("failures during demotion: %s", err.Error())
				}
			}
		}
		return err
	}

	return fmt.Errorf("unable to retrieve kv data: %s", err.Error())
}

// electionLoop is the main loop for the leader election.
func (b *Ballot) electionLoop() {
	err := b.session()
	if err != nil {
		log.WithFields(log.Fields{
			"caller": "electionLoop",
			"error":  err,
		}).Error("failed acquire session")
	}

	electionTicker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-electionTicker.C:
			err := b.election()
			if err != nil {
				log.WithFields(log.Fields{
					"caller": "electionLoop",
					"error":  err,
				}).Error("failed to run election")
			}
		case <-b.ctx.Done():
			return
		}
	}
}

// onDemote is called when the node is demoted from leader.
func (b *Ballot) onDemote() (err error) {
	b.leader.Store(false)
	err = b.updateServiceTags()
	if err != nil {
		return err
	}
	return err
}

// session is responsible for creating and renewing the session.
func (b *Ballot) session() (err error) {
	if b.sessionID.Load() != nil {
		currentSessionID := b.sessionID.Load().(string)
		sessionInfo, _, err := b.client.Session().Info(currentSessionID, nil)
		if err != nil {
			return err
		}
		if sessionInfo != nil {
			log.WithFields(log.Fields{
				"caller":  "session",
				"session": currentSessionID,
			}).Trace("returning cached session")
			return nil
		}
	}
	state, _, err := b.client.Agent().AgentHealthServiceByName(b.Name)
	if err != nil {
		return fmt.Errorf("failed to get health checks: %s", err)
	}
	if state == "critical" {
		return fmt.Errorf("service is in critical state, so I won't even try to create a session")
	}

	log.WithFields(log.Fields{
		"caller": "session",
	}).Trace("creating session")
	sessionID, _, err := b.client.Session().Create(&api.SessionEntry{
		Behavior:      "delete",
		ServiceChecks: b.makeServiceCheck(b.ServiceChecks),
		NodeChecks:    []string{"serfHealth"},
		TTL:           b.TTL.String(),
		LockDelay:     b.LockDelay,
	}, nil)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"caller": "session",
		"ID":     sessionID,
	}).Trace("storing session ID")
	b.sessionID.Store(sessionID)

	err = b.watch()
	if err != nil {
		log.WithFields(log.Fields{
			"caller": "watch",
		}).Error(err)
	}

	go func() {
		err := b.client.Session().RenewPeriodic(b.LockDelay.String(), sessionID, nil, b.ctx.Done())
		if err != nil {
			log.WithFields(log.Fields{
				"caller": "session",
				"error":  err,
			}).Warning("failed to renew session")
		}
	}()
	return err
}

// makeServiceChecks creates a service check from a string.
func (b *Ballot) makeServiceCheck(sc []string) (serviceChecks []api.ServiceCheck) {
	log.WithFields(log.Fields{
		"caller": "makeServiceCheck",
	}).Trace("Creating service checks from string")
	for _, check := range sc {
		log.WithFields(log.Fields{
			"caller":           "session",
			"makeServiceCheck": check,
		}).Trace("creating service check")
		serviceChecks = append(serviceChecks, api.ServiceCheck{
			ID: check,
		})
	}
	return serviceChecks
}

func (b *Ballot) IsLeader() bool {
	return b.leader.Load() && b.sessionID.Load() != nil
}

func (b *Ballot) getSessionKey() (session string, err error) {
	sessionKey, _, err := b.client.KV().Get(b.Key, nil)
	if err != nil {
		return session, err
	}
	if sessionKey == nil {
		return session, err
	}
	return sessionKey.Session, nil
}

// releaseSession releases the session.
func (b *Ballot) releaseSession() (err error) {
	if b.sessionID.Load() == nil {
		return nil
	}
	sessionID := b.sessionID.Load().(string)
	_, err = b.client.Session().Destroy(sessionID, nil)
	if err != nil {
		return err
	}
	b.sessionID = atomic.Value{}
	return err
}

// watch sets up a watch on a Consul KV pair and triggers actions on changes.
func (b *Ballot) watch() error {
	log.WithFields(log.Fields{
		"caller": "watch",
	}).Trace("Setting up watch on key")

	params := map[string]interface{}{
		"type": "key",
		"key":  b.Key,
	}
	if b.Token != "" {
		params["token"] = b.Token
	}

	wp, err := watch.Parse(params)
	if err != nil {
		return fmt.Errorf("failed to parse watch parameters: %s", err)
	}

	wp.Handler = func(idx uint64, data interface{}) {
		if data == nil {
			log.WithFields(log.Fields{
				"caller": "watch",
			}).Error("Received nil data on watch")
			return
		}

		kvPair, ok := data.(*api.KVPair)
		if !ok {
			log.WithFields(log.Fields{
				"caller": "watch",
			}).Error("Unexpected data type on watch")
			return
		}

		b.handleKVChange(kvPair)
	}

	go func() {
		if err := wp.RunWithClientAndHclog(b.client, nil); err != nil {
			log.WithFields(log.Fields{
				"caller": "watch",
				"error":  err,
			}).Error("Watch failed")
		}
	}()

	return nil
}

// handleKVChange handles changes to the KV pair being watched.
func (b *Ballot) handleKVChange(kvPair *api.KVPair) {
	electionPayload := &ElectionPayload{}
	if err := json.Unmarshal(kvPair.Value, electionPayload); err != nil {
		log.WithFields(log.Fields{
			"caller": "handleKVChange",
			"error":  err,
		}).Error("Failed to unmarshal election payload")
		return
	}

	if b.sessionID.Load() != nil && electionPayload.SessionID == b.sessionID.Load().(string) {
		b.executePromoteCommand(electionPayload)
	} else {
		b.executeDemoteCommand(electionPayload)
	}
}

// executePromoteCommand executes the command for promotion.
func (b *Ballot) executePromoteCommand(electionPayload *ElectionPayload) {
	if b.ExecOnPromote != "" {
		log.WithFields(log.Fields{
			"caller": "executePromoteCommand",
		}).Info("Executing promote command")
		if out, err := b.runCommand(b.ExecOnPromote, electionPayload); err != nil {
			log.WithFields(log.Fields{
				"caller": "executePromoteCommand",
				"error":  err,
			}).Error("Failed to execute promote command")
		} else {
			log.WithFields(log.Fields{
				"caller": "executePromoteCommand",
				"output": string(out),
			}).Info("Promote command executed successfully")
		}
	}
}

// executeDemoteCommand executes the command for demotion.
func (b *Ballot) executeDemoteCommand(electionPayload *ElectionPayload) {
	if b.ExecOnDemote != "" {
		log.WithFields(log.Fields{
			"caller": "executeDemoteCommand",
		}).Info("Executing demote command")
		if out, err := b.runCommand(b.ExecOnDemote, electionPayload); err != nil {
			log.WithFields(log.Fields{
				"caller": "executeDemoteCommand",
				"error":  err,
			}).Error("Failed to execute demote command")
		} else {
			log.WithFields(log.Fields{
				"caller": "executeDemoteCommand",
				"output": string(out),
			}).Info("Demote command executed successfully")
		}
	}
}
