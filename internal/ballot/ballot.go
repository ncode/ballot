package ballot

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"sync/atomic"
	"time"

	"github.com/google/shlex"
	"github.com/hashicorp/consul/api"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/exp/slices"
)

type CommandExecutor interface {
	CommandContext(ctx context.Context, name string, arg ...string) *exec.Cmd
}

type ConsulClient interface {
	Agent() *api.Agent
	Catalog() *api.Catalog
	Health() *api.Health
	KV() *api.KV
	Session() *api.Session
}

type ElectionPayload struct {
	Address   string
	Port      int
	SessionID string
}

type commandExecutor struct{}

func (c *commandExecutor) CommandContext(ctx context.Context, name string, arg ...string) *exec.Cmd {
	return exec.CommandContext(ctx, name, arg...)
}

// New returns a new Ballot instance.
func New(ctx context.Context, name string) (*Ballot, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is required")
	}
	consulConfig := api.DefaultConfig()
	consulConfig.Token = viper.GetString("consul.token")
	consulConfig.Address = viper.GetString("consul.address")
	client, err := api.NewClient(consulConfig)
	if err != nil {
		return nil, err
	}

	b := &Ballot{}
	err = viper.UnmarshalKey(fmt.Sprintf("election.services.%s", name), b)
	if err != nil {
		return nil, err
	}
	b.client = client
	b.leader.Store(false)
	b.Token = consulConfig.Token
	b.ctx = ctx
	b.executor = &commandExecutor{}

	// Ensure that required fields are set
	if b.ID == "" {
		return nil, fmt.Errorf("service ID is required; please set the 'id' field in the configuration")
	}
	if b.Name == "" {
		b.Name = name
	}
	if b.LockDelay == 0 {
		b.LockDelay = 3 * time.Second
	}
	if b.TTL == 0 {
		b.TTL = 10 * time.Second
	}

	return b, nil
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
	sessionID     atomic.Value    // stores *string
	leader        atomic.Bool     `mapstructure:"-"`
	client        ConsulClient    `mapstructure:"-"`
	ctx           context.Context `mapstructure:"-"`
	executor      CommandExecutor `mapstructure:"-"`
}

// Copy *api.AgentService to *api.AgentServiceRegistration
func (b *Ballot) copyServiceToRegistration(service *api.AgentService) *api.AgentServiceRegistration {
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

// Copy *api.CatalogService to *api.CatalogRegistration
func (b *Ballot) copyCatalogServiceToRegistration(service *api.CatalogService) *api.CatalogRegistration {
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

// getService returns the registered service.
func (b *Ballot) getService() (service *api.AgentService, catalogServices []*api.CatalogService, err error) {
	agent := b.client.Agent()
	if b.ID == "" {
		return nil, nil, fmt.Errorf("service ID is empty; please ensure it is set in the configuration")
	}
	service, _, err = agent.Service(b.ID, &api.QueryOptions{})
	if err != nil {
		return nil, nil, err
	}
	if service == nil {
		return nil, nil, fmt.Errorf("service %s not found", b.ID)
	}
	catalog := b.client.Catalog()
	catalogServices, _, err = catalog.Service(b.Name, b.PrimaryTag, &api.QueryOptions{})
	if err != nil {
		return service, nil, err
	}
	return service, catalogServices, nil
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
	cmd := b.executor.CommandContext(b.ctx, args[0], args[1:]...)
	cmd.Env = append(cmd.Env, fmt.Sprintf("ADDRESS=%s", electionPayload.Address))
	cmd.Env = append(cmd.Env, fmt.Sprintf("PORT=%d", electionPayload.Port))
	cmd.Env = append(cmd.Env, fmt.Sprintf("SESSIONID=%s", electionPayload.SessionID))
	return cmd.CombinedOutput()
}

// updateServiceTags updates the service tags.
func (b *Ballot) updateServiceTags(isLeader bool) error {
	service, _, err := b.getService()
	if err != nil {
		return err
	}

	// Get a copy of the current service registration
	registration := b.copyServiceToRegistration(service)
	if registration == nil {
		return fmt.Errorf("service registration is nil")
	}

	// Determine if the primary tag is already present
	hasPrimaryTag := slices.Contains(registration.Tags, b.PrimaryTag)

	// Update tags based on leadership status
	if isLeader && !hasPrimaryTag {
		// Add primary tag if not present and this node is the leader
		registration.Tags = append(registration.Tags, b.PrimaryTag)
	} else if !isLeader && hasPrimaryTag {
		// Remove primary tag if present and this node is not the leader
		index := slices.Index(registration.Tags, b.PrimaryTag)
		registration.Tags = append(registration.Tags[:index], registration.Tags[index+1:]...)
	} else {
		// No changes needed
		return nil
	}

	// Run the command associated with the new leadership status
	var command string
	if isLeader {
		command = b.ExecOnPromote
	} else {
		command = b.ExecOnDemote
	}
	if command != "" && b.executor != nil {
		go func(isLeader bool, command string) {
			defer func() {
				if r := recover(); r != nil {
					log.WithFields(log.Fields{
						"caller": "updateServiceTags",
						"error":  r,
					}).Error("Recovered from panic in command execution")
				}
			}()

			// Run the command in a separate goroutine
			ctx, cancel := context.WithTimeout(b.ctx, (b.TTL+b.LockDelay)*2)
			defer cancel()
			payload, payloadErr := b.waitForNextValidSessionData(ctx)
			if payloadErr != nil {
				log.WithFields(log.Fields{
					"caller": "updateServiceTags",
					"error":  payloadErr,
				}).Error("Failed to get session data")
				return
			}
			output, cmdErr := b.runCommand(command, payload)
			if cmdErr != nil {
				log.WithFields(log.Fields{
					"caller":   "updateServiceTags",
					"isLeader": isLeader,
					"error":    cmdErr,
				}).Error("Failed to run command")
			} else {
				log.WithFields(log.Fields{
					"caller":   "updateServiceTags",
					"isLeader": isLeader,
					"output":   string(output),
				}).Info("Ran command")
			}
		}(isLeader, command)
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
			if catalogRegistration == nil || catalogRegistration.Service == nil {
				continue
			}
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

// election is the main logic for the leader election.
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
	sessionIDPtr := b.sessionID.Load().(*string)
	if sessionIDPtr == nil {
		return fmt.Errorf("session ID is nil")
	}
	electionPayload := &ElectionPayload{
		Address:   service.Address,
		Port:      service.Port,
		SessionID: *sessionIDPtr,
	}

	// Attempt to acquire leadership
	if !b.leader.Load() && sessionIDPtr != nil {
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

// attemptLeadershipAcquisition attempts to acquire leadership.
func (b *Ballot) attemptLeadershipAcquisition(electionPayload *ElectionPayload) (bool, *api.WriteMeta, error) {
	payload, err := json.Marshal(electionPayload)
	if err != nil {
		return false, nil, fmt.Errorf("failed to marshal election payload: %s", err)
	}

	sessionIDPtr := b.sessionID.Load().(*string)
	if sessionIDPtr == nil {
		return false, nil, fmt.Errorf("session ID is nil")
	}

	content := &api.KVPair{
		Key:     b.Key,
		Session: *sessionIDPtr,
		Value:   payload,
	}

	return b.client.KV().Acquire(content, nil)
}

// verifyAndUpdateLeadershipStatus checks the current session data and updates the leadership status.
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

	sessionIDPtr := b.sessionID.Load().(*string)
	isCurrentLeader := sessionIDPtr != nil && currentSessionData.SessionID == *sessionIDPtr
	return b.updateLeadershipStatus(isCurrentLeader)
}

// Run is the main loop for the leader election.
func (b *Ballot) Run() (err error) {
	tickerInterval := b.TTL / 2
	if tickerInterval < time.Second {
		tickerInterval = time.Second
	}
	electionTicker := time.NewTicker(tickerInterval)
	defer electionTicker.Stop()

	for {
		select {
		case <-electionTicker.C:
			err := b.election()
			if err != nil {
				log.WithFields(log.Fields{
					"caller": "Run",
					"error":  err,
				}).Error("Failed to run election")
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
	err := b.updateServiceTags(isLeader)
	if err != nil {
		return err
	}

	return nil
}

// handleServiceCriticalState is called when the service is in a critical state.
func (b *Ballot) handleServiceCriticalState() error {
	healthChecks, _, err := b.client.Health().Checks(b.Name, nil)
	if err != nil {
		return fmt.Errorf("failed to get health checks: %s", err)
	}

	// Determine the aggregate status
	state := "passing"
	for _, check := range healthChecks {
		if check.Status == "critical" {
			state = "critical"
			break
		} else if check.Status == "warning" {
			state = "warning"
		}
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
		return fmt.Errorf("service is in critical state, skipping the election")
	}
	return nil
}

// session is responsible for creating and renewing the session.
func (b *Ballot) session() (err error) {
	if b.client == nil {
		return fmt.Errorf("consul client is required")
	}
	sessionIDPtr := b.sessionID.Load().(*string)
	if sessionIDPtr != nil {
		currentSessionID := *sessionIDPtr
		sessionInfo, _, err := b.client.Session().Info(currentSessionID, nil)
		if err != nil {
			return err
		}
		if sessionInfo != nil {
			log.WithFields(log.Fields{
				"caller":  "session",
				"session": currentSessionID,
			}).Trace("Returning cached session")
			return nil
		}
	}

	log.WithFields(log.Fields{
		"caller": "session",
	}).Trace("Creating new session")
	sessionID, _, err := b.client.Session().Create(&api.SessionEntry{
		Behavior:  "delete",
		Checks:    append(b.ServiceChecks, "serfHealth"),
		TTL:       b.TTL.String(),
		LockDelay: b.LockDelay,
	}, nil)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"caller": "session",
		"ID":     sessionID,
	}).Trace("Storing session ID")
	b.sessionID.Store(&sessionID)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.WithFields(log.Fields{
					"caller": "session",
					"error":  r,
				}).Error("Recovered from panic in session renewal")
			}
		}()

		err := b.client.Session().RenewPeriodic(b.TTL.String(), sessionID, nil, b.ctx.Done())
		if err != nil {
			log.WithFields(log.Fields{
				"caller": "session",
				"error":  err,
			}).Warning("Failed to renew session")
			b.sessionID.Store((*string)(nil))
		}
	}()
	return err
}

func (b *Ballot) IsLeader() bool {
	sessionIDValue := b.sessionID.Load()
	if sessionIDValue == nil {
		return false
	}
	sessionIDPtr, ok := sessionIDValue.(*string)
	if !ok || sessionIDPtr == nil {
		return false
	}
	return b.leader.Load()
}

func (b *Ballot) waitForNextValidSessionData(ctx context.Context) (data *ElectionPayload, err error) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			data, err := b.getSessionData()
			if err != nil {
				return data, err
			}
			if data != nil {
				return data, nil
			}
		case <-ctx.Done():
			return data, ctx.Err()
		}
	}
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
	sessionIDPtr := b.sessionID.Load().(*string)
	if sessionIDPtr == nil {
		return nil
	}
	sessionID := *sessionIDPtr
	_, err = b.client.Session().Destroy(sessionID, nil)
	if err != nil {
		return err
	}
	b.sessionID.Store((*string)(nil))
	return err
}
