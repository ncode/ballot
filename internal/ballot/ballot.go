package ballot

import (
	"encoding/json"
	"fmt"
	"github.com/google/shlex"
	"os/exec"
	"sync/atomic"
	"time"

	"github.com/hashicorp/consul/api"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/exp/slices"
	"golang.org/x/net/context"
)

type CommandExecutor interface {
	Command(name string, arg ...string) *exec.Cmd
}

type ConsulClient interface {
	Agent() *api.Agent
	Catalog() *api.Catalog
	KV() *api.KV
	Session() *api.Session
}

type ElectionPayload struct {
	Address   string
	Port      int
	SessionID string
}

// New returns a new Ballot instance.
func New(ctx context.Context, name string) (b *Ballot, err error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is required")
	}
	consulConfig := api.DefaultConfig()
	consulConfig.Token = viper.GetString("consul.token")
	consulConfig.Address = viper.GetString("consul.address")
	client, err := api.NewClient(consulConfig)
	if err != nil {
		return b, err
	}

	b = &Ballot{}
	err = viper.UnmarshalKey(fmt.Sprintf("election.services.%s", name), b)
	if err != nil {
		return b, err
	}
	b.client = client
	b.leader.Store(false)
	b.Token = consulConfig.Token
	b.ctx = ctx

	b.Name = name
	if b.LockDelay == 0 {
		b.LockDelay = 3 * time.Second
	}
	if b.TTL == 0 {
		b.TTL = 10 * time.Second
	}

	return b, err
}

// Ballot is a struct that holds the configuration for the leader election.
type Ballot struct {
	Name          string          `mapstructure:"-"`
	ID            string          `mapstructure:"id"`
	Key           string          `mapstructure:"key"`
	ServiceChecks []string        `mapstructure:"serviceChecks"`
	Token         string          `mapstructure:"consul.token"`
	ExecOnPromote string          `mapstructure:"execOnPromote"`
	ExecOnDemote  string          `mapstructure:"execOnDemote"`
	PrimaryTag    string          `mapstructure:"primaryTag"`
	ConsulAddress string          `mapstructure:"consul.address"`
	TTL           time.Duration   `mapstructure:"ttl"`
	LockDelay     time.Duration   `mapstructure:"lockDelay"`
	sessionID     atomic.Value    `mapstructure:"-"`
	leader        atomic.Bool     `mapstructure:"-"`
	client        ConsulClient    `mapstructure:"-"`
	ctx           context.Context `mapstructure:"-"`
	exec          CommandExecutor `mapstructure:"-"`
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

// runCommand runs a command and returns the output.
func (b *Ballot) runCommand(command string, electionPayload *ElectionPayload) ([]byte, error) {
	log.WithFields(log.Fields{
		"caller": "runCommand",
	}).Info("Running command: ", command)
	args, err := shlex.Split(command)
	if err != nil {
		return nil, err
	}
	cmd := b.exec.Command(args[0], args[1:]...)
	cmd.Env = append(cmd.Env, fmt.Sprintf("ADDRESS=%s", electionPayload.Address))
	cmd.Env = append(cmd.Env, fmt.Sprintf("PORT=%d", electionPayload.Port))
	cmd.Env = append(cmd.Env, fmt.Sprintf("SESSIONID=%s", electionPayload.SessionID))
	return cmd.Output()
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

// cleanup is called by the winning service to ensure that the primary tag is removed from all other instances of the service.
func (b *Ballot) cleanup(payload *ElectionPayload) error {
	if !b.IsLeader() {
		// If this instance is not the leader, do not perform cleanup
		return nil
	}

	catalogServices, _, err := b.client.Catalog().Service(b.Name, "", nil)
	if err != nil {
		return fmt.Errorf("failed to retrieve services from the catalog: %s", err)
	}

	for _, service := range catalogServices {
		// Skip cleaning up the leader's own service
		if service.ServiceAddress == payload.Address && service.ServicePort == payload.Port {
			continue
		}

		// Check if the primary tag is present
		primaryTagIndex := slices.Index(service.ServiceTags, b.PrimaryTag)
		if primaryTagIndex != -1 {
			// Remove the primary tag
			updatedTags := append(service.ServiceTags[:primaryTagIndex], service.ServiceTags[primaryTagIndex+1:]...)

			// Prepare the catalog registration for update
			catalogRegistration := b.copyCatalogServiceToRegistration(service)
			catalogRegistration.Service.Tags = updatedTags

			// Update the catalog service
			_, err := b.client.Catalog().Register(catalogRegistration, nil)
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
	}
	return nil
}

func (b *Ballot) election() (err error) {
	err = b.handleServiceCriticalState()
	if err != nil {
		return err
	}

	service, _, err := b.getService()
	if err != nil {
		return fmt.Errorf("failed to get service: %s", err)
	}

	// Session validation and renewal
	err = b.session()
	if err != nil {
		return fmt.Errorf("failed to create session: %s", err)
	}

	// Prepare the election payload
	electionPayload := &ElectionPayload{
		Address:   service.Address,
		Port:      service.Port,
		SessionID: b.sessionID.Load().(string),
	}

	// Attempt to acquire leadership
	if !b.leader.Load() && b.sessionID.Load() != nil {
		_, _, err = b.attemptLeadershipAcquisition(electionPayload)
		if err != nil {
			return fmt.Errorf("failed to acquire lock: %s", err)
		}
	}

	err = b.cleanup(electionPayload)
	if err != nil {
		return fmt.Errorf("failed to cleanup: %s", err)
	}

	// Check leadership status and respond accordingly
	return b.verifyAndUpdateLeadershipStatus()
}

func (b *Ballot) attemptLeadershipAcquisition(electionPayload *ElectionPayload) (bool, *api.WriteMeta, error) {
	payload, err := json.Marshal(electionPayload)
	if err != nil {
		return false, nil, fmt.Errorf("failed to marshal election payload: %s", err)
	}

	content := &api.KVPair{
		Key:     b.Key,
		Session: b.sessionID.Load().(string),
		Value:   payload,
	}

	return b.client.KV().Acquire(content, nil)
}

func (b *Ballot) verifyAndUpdateLeadershipStatus() error {
	currentSessionData, err := b.getSessionData()
	if err != nil {
		return fmt.Errorf("failed to get session data: %s", err)
	}

	if currentSessionData == nil {
		log.WithFields(log.Fields{
			"caller": "verifyAndUpdateLeadershipStatus",
		}).Debug("current session data is nil, skipping election check")
		return nil
	}

	isCurrentLeader := b.sessionID.Load() != nil && currentSessionData.SessionID == b.sessionID.Load().(string)
	return b.updateLeadershipStatus(isCurrentLeader)
}

// Run is the main loop for the leader election.
func (b *Ballot) Run() (err error) {
	electionTicker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-electionTicker.C:
			err := b.election()
			if err != nil {
				log.WithFields(log.Fields{
					"caller": "Run",
					"error":  err,
				}).Error("failed to run election")
			}
		case <-b.ctx.Done():
			return err
		}
	}
}

// updateLeadershipStatus is called when there is a change in leadership status.
func (b *Ballot) updateLeadershipStatus(isLeader bool) error {
	// Update leader status
	b.leader.Store(isLeader)

	// Update service tags based on leadership status
	err := b.updateServiceTags()
	if err != nil {
		return err
	}

	return nil
}

// handleServiceCriticalState is called when the service is in a critical state.
func (b *Ballot) handleServiceCriticalState() error {
	state, _, err := b.client.Agent().AgentHealthServiceByName(b.Name)
	if err != nil {
		return fmt.Errorf("failed to get health checks: %s", err)
	}

	if state == "critical" {
		err := b.releaseSession()
		if err != nil {
			return fmt.Errorf("failed to release session for service in critical state: %s", err)
		}
		err = b.updateLeadershipStatus(false)
		if err != nil {
			return fmt.Errorf("failed to update leadership status for service in critical state: %s", err)
		}
		return fmt.Errorf("service is in critical state, so I'm skipping the election")
	}
	return nil
}

// session is responsible for creating and renewing the session.
func (b *Ballot) session() (err error) {
	if b.client == nil {
		return fmt.Errorf("consul client is required")
	}
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

func (b *Ballot) getSessionData() (data *ElectionPayload, err error) {
	sessionKey, _, err := b.client.KV().Get(b.Key, nil)
	if err != nil {
		return data, err
	}
	if sessionKey == nil {
		return data, err
	}
	err = json.Unmarshal(sessionKey.Value, &data)
	if err != nil {
		return data, err
	}

	return data, nil
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
