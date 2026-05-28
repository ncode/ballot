package ballot

import (
	"context"
	"fmt"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/consul/api"
	log "github.com/sirupsen/logrus"
)

type CommandExecutor interface {
	CommandContext(ctx context.Context, name string, arg ...string) *exec.Cmd
}

type consulClient struct {
	client *api.Client
}

func (c *consulClient) Agent() AgentInterface {
	return c.client.Agent()
}

func (c *consulClient) Catalog() CatalogInterface {
	return c.client.Catalog()
}

func (c *consulClient) Health() HealthInterface {
	return c.client.Health()
}

func (c *consulClient) Session() SessionInterface {
	return c.client.Session()
}

func (c *consulClient) KV() KVInterface {
	return c.client.KV()
}

// AgentInterface is an interface that wraps the Consul agent methods.
type AgentInterface interface {
	Service(serviceID string, q *api.QueryOptions) (*api.AgentService, *api.QueryMeta, error)
	ServiceRegister(service *api.AgentServiceRegistration) error
}

// CatalogInterface is an interface that wraps the Consul catalog methods.
type CatalogInterface interface {
	Service(serviceName, tag string, q *api.QueryOptions) ([]*api.CatalogService, *api.QueryMeta, error)
	Register(reg *api.CatalogRegistration, w *api.WriteOptions) (*api.WriteMeta, error)
}

// HealthInterface is an interface that wraps the Consul health methods.
type HealthInterface interface {
	Checks(service string, q *api.QueryOptions) (api.HealthChecks, *api.QueryMeta, error)
}

// SessionInterface is an interface that wraps the Consul session methods.
type SessionInterface interface {
	Create(se *api.SessionEntry, q *api.WriteOptions) (string, *api.WriteMeta, error)
	Destroy(sessionID string, q *api.WriteOptions) (*api.WriteMeta, error)
	Info(sessionID string, q *api.QueryOptions) (*api.SessionEntry, *api.QueryMeta, error)
	RenewPeriodic(initialTTL string, sessionID string, q *api.WriteOptions, doneCh <-chan struct{}) error
}

// KVInterface is an interface that wraps the Consul KV methods.
type KVInterface interface {
	Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error)
	Put(p *api.KVPair, q *api.WriteOptions) (*api.WriteMeta, error)
	Acquire(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error)
}

type ConsulClient interface {
	Agent() AgentInterface
	Catalog() CatalogInterface
	Health() HealthInterface
	KV() KVInterface
	Session() SessionInterface
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
func New(ctx context.Context, cfg RuntimeConfig) (*Ballot, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is required")
	}
	if err := validateRuntimeConfig(cfg); err != nil {
		return nil, err
	}
	consulConfig := api.DefaultConfig()
	consulConfig.Token = cfg.ConsulToken
	consulConfig.Address = cfg.ConsulAddress
	client, err := api.NewClient(consulConfig)
	if err != nil {
		return nil, err
	}

	b := &Ballot{
		Name:          cfg.Name,
		ID:            cfg.ID,
		Key:           cfg.Key,
		ServiceChecks: cfg.ServiceChecks,
		Token:         consulConfig.Token,
		ExecOnPromote: cfg.ExecOnPromote,
		ExecOnDemote:  cfg.ExecOnDemote,
		PrimaryTag:    cfg.PrimaryTag,
		ConsulAddress: consulConfig.Address,
		TTL:           cfg.TTL,
		LockDelay:     cfg.LockDelay,
		client:        &consulClient{client: client},
		ctx:           ctx,
		executor:      &commandExecutor{},
	}
	b.leader.Store(false)
	b.hooks = NewLeadershipHooks(b.executor)

	return b, nil
}

// Ballot is a struct that holds the configuration for the leader election.
type Ballot struct {
	Name             string            `mapstructure:"-"`
	ID               string            `mapstructure:"id"`
	Key              string            `mapstructure:"key"`
	ServiceChecks    []string          `mapstructure:"serviceChecks"`
	Token            string            `mapstructure:"consul.token"`
	ExecOnPromote    string            `mapstructure:"execOnPromote"`
	ExecOnDemote     string            `mapstructure:"execOnDemote"`
	PrimaryTag       string            `mapstructure:"primaryTag"`
	ConsulAddress    string            `mapstructure:"consul.address"`
	TTL              time.Duration     `mapstructure:"ttl"`
	LockDelay        time.Duration     `mapstructure:"lockDelay"`
	sessionID        atomic.Value      // stores *string
	leader           atomic.Bool       `mapstructure:"-"`
	client           ConsulClient      `mapstructure:"-"`
	ctx              context.Context   `mapstructure:"-"`
	executor         CommandExecutor   `mapstructure:"-"`
	hooks            *LeadershipHooks  `mapstructure:"-"`
	hookResultsMu    sync.Mutex        `mapstructure:"-"`
	hookResults      chan HookResult   `mapstructure:"-"`
	sessionLifecycle *SessionLifecycle `mapstructure:"-"`
}

// getService returns the registered service.
func (b *Ballot) getService() (service *api.AgentService, catalogServices []*api.CatalogService, err error) {
	return b.interaction().LocalService()
}

// updateServiceTags updates the service tags.
func (b *Ballot) updateServiceTags(isLeader bool) error {
	changed, err := b.interaction().ApplyPrimaryTag(isLeader)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}

	var command string
	if isLeader {
		command = b.ExecOnPromote
	} else {
		command = b.ExecOnDemote
	}
	if command == "" || b.executor == nil {
		return nil
	}
	result := b.executeLeadershipHook(isLeader, command)
	if result.Err != nil {
		log.WithFields(log.Fields{
			"caller":   "updateServiceTags",
			"isLeader": isLeader,
			"error":    result.Err,
		}).Error("Failed to run command")
	} else if !result.Skipped {
		log.WithFields(log.Fields{
			"caller":   "updateServiceTags",
			"isLeader": isLeader,
			"output":   string(result.Output),
		}).Info("Ran command")
	}
	return nil
}

// cleanup is called by the winning service to ensure that the primary tag is removed from all other instances of the service.
func (b *Ballot) cleanup(payload *ElectionPayload) error {
	return b.interaction().CleanupStalePrimaryTags(payload, b.IsLeader())
}

// election is the main logic for the leader election.
func (b *Ballot) election() error {
	return NewElectionStep(b).Run().Err
}

// attemptLeadershipAcquisition attempts to acquire leadership.
func (b *Ballot) attemptLeadershipAcquisition(electionPayload *ElectionPayload) (bool, *api.WriteMeta, error) {
	sessionIDPtr, ok := b.getSessionID()
	if !ok || sessionIDPtr == nil {
		return false, nil, fmt.Errorf("session ID is nil")
	}
	return b.interaction().AcquireLeadership(electionPayload, *sessionIDPtr)
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

	sessionIDPtr, ok := b.getSessionID()
	isCurrentLeader := ok && sessionIDPtr != nil && currentSessionData.SessionID == *sessionIDPtr
	return b.updateLeadershipStatus(isCurrentLeader)
}

// Run is the main loop for the leader election.
func (b *Ballot) Run() error {
	tickerInterval := b.TTL / 2
	if tickerInterval < time.Second {
		tickerInterval = time.Second
	}

	// Run the first election immediately
	if err := b.election(); err != nil {
		log.WithFields(log.Fields{
			"caller": "Run",
			"error":  err,
		}).Error("Failed to run initial election")
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
			return nil
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
	state, err := b.interaction().HealthState()
	if err != nil {
		return err
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
func (b *Ballot) session() error {
	if b.client == nil {
		return fmt.Errorf("consul client is required")
	}
	_, err := b.lifecycle().ActiveSession()
	return err
}

func (b *Ballot) getSessionID() (*string, bool) {
	sessionIDValue := b.sessionID.Load()
	if sessionIDValue == nil {
		return nil, false
	}
	sessionIDPtr, ok := sessionIDValue.(*string)
	return sessionIDPtr, ok
}

func (b *Ballot) runtimeConfig() RuntimeConfig {
	return RuntimeConfig{
		Name:          b.Name,
		ID:            b.ID,
		Key:           b.Key,
		ServiceChecks: b.ServiceChecks,
		ConsulToken:   b.Token,
		ExecOnPromote: b.ExecOnPromote,
		ExecOnDemote:  b.ExecOnDemote,
		PrimaryTag:    b.PrimaryTag,
		ConsulAddress: b.ConsulAddress,
		TTL:           b.TTL,
		LockDelay:     b.LockDelay,
	}
}

func (b *Ballot) interaction() *ConsulElectionInteraction {
	return NewConsulElectionInteraction(b.client, b.runtimeConfig())
}

func (b *Ballot) lifecycle() *SessionLifecycle {
	if b.sessionLifecycle == nil {
		b.sessionLifecycle = NewSessionLifecycle(b.ctx, b.client.Session(), &b.sessionID, b.runtimeConfig())
	}
	return b.sessionLifecycle
}

func (b *Ballot) leadershipHooks() *LeadershipHooks {
	if b.hooks == nil {
		b.hooks = NewLeadershipHooks(b.executor)
	}
	return b.hooks
}

func (b *Ballot) executeLeadershipHook(isLeader bool, command string) HookResult {
	transition := HookDemote
	if isLeader {
		transition = HookPromote
	}

	baseCtx := b.ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	ctx, cancel := context.WithTimeout(baseCtx, HookTimeout(b.TTL, b.LockDelay))
	defer cancel()

	payload, err := b.waitForNextValidSessionData(ctx)
	if err != nil {
		result := HookResult{
			Transition: transition,
			Command:    command,
			Err:        err,
			TimedOut:   ctx.Err() == context.DeadlineExceeded,
		}
		b.publishHookResult(result)
		return result
	}

	result := b.leadershipHooks().Execute(baseCtx, HookRequest{
		Transition: transition,
		Command:    command,
		Payload:    payload,
		Timeout:    HookTimeout(b.TTL, b.LockDelay),
	})
	b.publishHookResult(result)
	return result
}

func (b *Ballot) publishHookResult(result HookResult) {
	results := b.hookResultsChan()
	select {
	case results <- result:
	default:
	}
}

func (b *Ballot) hookResultsChan() chan HookResult {
	b.hookResultsMu.Lock()
	defer b.hookResultsMu.Unlock()
	if b.hookResults == nil {
		b.hookResults = make(chan HookResult, 8)
	}
	return b.hookResults
}

func (b *Ballot) IsLeader() bool {
	sessionIDPtr, ok := b.getSessionID()
	if !ok || sessionIDPtr == nil {
		return false
	}
	return b.leader.Load()
}

func (b *Ballot) waitForNextValidSessionData(ctx context.Context) (*ElectionPayload, error) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		data, err := b.getSessionData()
		if err != nil {
			return data, err
		}
		if data != nil {
			return data, nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (b *Ballot) getSessionData() (*ElectionPayload, error) {
	return b.interaction().LockPayload()
}

// releaseSession releases the session.
func (b *Ballot) releaseSession() error {
	if b.sessionLifecycle != nil {
		return b.sessionLifecycle.Release()
	}
	sessionIDPtr, ok := b.getSessionID()
	if !ok || sessionIDPtr == nil {
		return nil
	}
	sessionID := *sessionIDPtr
	b.sessionID.Store((*string)(nil))
	_, err := b.client.Session().Destroy(sessionID, nil)
	return err
}
