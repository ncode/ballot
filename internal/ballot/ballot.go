package ballot

import (
	"fmt"
	"github.com/google/shlex"
	"github.com/hashicorp/consul/api"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
	"golang.org/x/net/context"
	"os/exec"
	"sync/atomic"
	"time"
)

// New returns a new Ballot instance.
func New(name string, key string, serviceChecks []string, token string, execOnPromote string, execOnDemote string, primaryTag string, timeout time.Duration, lockDelay time.Duration) (b *Ballot, err error) {
	b = &Ballot{
		Name:          name,
		Key:           key,
		ServiceChecks: serviceChecks,
		Token:         token,
		ExecOnPromote: execOnPromote,
		ExecOnDemote:  execOnDemote,
		PrimaryTag:    primaryTag,
		Timeout:       timeout,
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
	Timeout       time.Duration   `mapstructure:"timeout"`
	LockDelay     time.Duration   `mapstructure:"lockDelay"`
	sessionID     atomic.Value    `mapstructure:"-"`
	leader        atomic.Bool     `mapstructure:"-"`
	client        *api.Client     `mapstructure:"-"`
	ctx           context.Context `mapstructure:"-"`
}

// runCommand runs a command and returns the output.
func (b *Ballot) runCommand(command string) ([]byte, error) {
	log.WithFields(log.Fields{
		"caller": "runCommand",
	}).Info("Running command: ", command)
	args, err := shlex.Split(command)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(args[0], args[1:]...)
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
	err = b.session()
	if err != nil {
		return err
	}
	b.electionLoop()

	return nil
}

// updateServiceTags updates the service tags.
func (b *Ballot) updateServiceTags() error {
	agent := b.client.Agent()
	services, err := agent.Services()
	if err != nil {
		return err
	}

	registration := b.copyServiceToRegistration(services[b.ID])
	log.WithFields(log.Fields{
		"caller":  "updateServiceTags",
		"service": b.ID,
		"tags":    registration.Tags,
	}).Debug("current service tags")
	if b.IsLeader() && !slices.Contains(registration.Tags, b.PrimaryTag) {
		registration.Tags = append(registration.Tags, b.PrimaryTag)
		log.WithFields(log.Fields{
			"caller":  "updateServiceTags",
			"service": b.ID,
			"tags":    registration.Tags,
		}).Debug("new service tags")
	} else {
		p := slices.Index(registration.Tags, b.PrimaryTag)
		if p == -1 {
			return nil
		}
		registration.Tags = slices.Delete(registration.Tags, p, p+1)
		log.WithFields(log.Fields{
			"caller":  "updateServiceTags",
			"service": b.ID,
			"tags":    registration.Tags,
		}).Debug("new service tags")
	}

	err = agent.ServiceRegister(registration)
	if err != nil {
		return err
	}

	return err
}

// onPromote is called when the node is promoted to leader.
func (b *Ballot) onPromote() (err error) {
	b.leader.Store(true)
	err = b.updateServiceTags()
	if err != nil {
		return err
	}
	if b.ExecOnPromote != "" {
		out, err := b.runCommand(b.ExecOnPromote)
		if err != nil {
			return err
		}
		log.WithFields(log.Fields{
			"state":  "leader",
			"caller": "OnPromote",
		}).Info(string(out))
	}
	return err
}

// election is responsible for the leader election.
func (b *Ballot) election() (err error) {
	// Check if we are the leader
	session, err := b.getSessionKey()
	if err != nil {
		return fmt.Errorf("unable to retrieve kv data: %s", err.Error())
	}
	if session == b.sessionID.Load() {
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

// electionLoop is the main loop for the leader election.
func (b *Ballot) electionLoop() {
	electionTicker := time.NewTicker(b.Timeout)
	sessionTicker := time.NewTicker(b.Timeout / 2)
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
		case <-sessionTicker.C:
			if !b.IsLeader() {
				sessionID := b.sessionID.Load().(string)
				content := &api.KVPair{
					Key:     b.Key,
					Session: sessionID,
					Value:   []byte(sessionID),
				}
				_, _, err := b.client.KV().Acquire(content, nil)
				log.WithFields(log.Fields{
					"caller": "electionLoop",
					"error":  err,
				}).Error("failed acquire lock")
			}
		case <-b.ctx.Done():
			return
		}
		time.Sleep(b.Timeout)
	}
}

// onDemote is called when the node is demoted from leader.
func (b *Ballot) onDemote() (err error) {
	b.leader.Store(false)
	err = b.updateServiceTags()
	if err != nil {
		return err
	}
	if b.ExecOnDemote != "" {
		out, err := b.runCommand(b.ExecOnDemote)
		if err != nil {
			return err
		}
		log.WithFields(log.Fields{
			"state":  "follower",
			"caller": "OnDemote",
		}).Info(string(out))
	}
	return err
}

// session is responsible for creating and renewing the session.
func (b *Ballot) session() (err error) {
	log.WithFields(log.Fields{
		"caller": "session",
	}).Trace("Creating session")
	sessionID, _, err := b.client.Session().Create(&api.SessionEntry{
		Behavior:      "delete",
		ServiceChecks: b.makeServiceCheck(b.ServiceChecks),
		NodeChecks:    []string{"serfHealth"},
		TTL:           b.Timeout.String(),
		LockDelay:     b.LockDelay,
	}, nil)
	log.WithFields(log.Fields{
		"caller": "session",
		"ID":     sessionID,
	}).Trace("storing session ID")
	b.sessionID.Store(sessionID)
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
	return b.leader.Load()
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
