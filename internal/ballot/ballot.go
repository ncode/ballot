package ballot

import (
	"encoding/json"
	"fmt"
	"github.com/google/shlex"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
	"golang.org/x/net/context"
	"os/exec"
	"sync/atomic"
	"time"
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
	b.electionLoop()

	return nil
}

// getService returns the registered service.
func (b *Ballot) getService() (service *api.AgentService, err error) {
	agent := b.client.Agent()
	services, err := agent.Services()
	if err != nil {
		return service, err
	}
	service, ok := services[b.ID]
	if !ok {
		return service, fmt.Errorf("service %s not found", b.ID)
	}
	return service, err
}

// updateServiceTags updates the service tags.
func (b *Ballot) updateServiceTags() error {
	service, err := b.getService()
	if err != nil {
		return err
	}
	registration := b.copyServiceToRegistration(service)
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

	agent := b.client.Agent()
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
		b.releaseSession()
		return fmt.Errorf("failed to update service tags: %s", err)
	}
	return err
}

// election is responsible for the leader election.
func (b *Ballot) election() (err error) {
	err = b.session()
	if err != nil {
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
			service, err := b.getService()
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
	sessionTicker := time.NewTicker(3 * time.Second)
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

	go func() {
		err := b.watch()
		if err != nil {
			log.WithFields(log.Fields{
				"caller": "watch",
			}).Error(err)
		}
	}()

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
	b.sessionID.Store(nil)
	return err
}

// watch is responsible for watching the key and run a command when notified by changes
func (b *Ballot) watch() error {
	log.WithFields(log.Fields{
		"caller": "watch",
	}).Trace("watching key")
	params := make(map[string]interface{})
	params["type"] = "key"
	params["key"] = b.Key
	if b.Token != "" {
		params["token"] = b.Token
	}

	// Watch for changes
	wp, err := watch.Parse(params)
	if err != nil {
		return err
	}
	wp.Handler = func(idx uint64, data interface{}) {
		electionPayload := &ElectionPayload{}
		response, ok := data.(*api.KVPair)
		if !ok {
			return
		}
		err = json.Unmarshal(response.Value, electionPayload)
		if err != nil {
			log.WithFields(log.Fields{
				"caller": "watch",
				"error":  err,
			}).Error("failed to unmarshal election payload")
		}
		if b.sessionID.Load() != nil && (electionPayload.SessionID == b.sessionID.Load().(string)) {
			if b.ExecOnPromote != "" {
				log.WithFields(log.Fields{
					"caller": "watch",
				}).Info("executing command on promote")
				out, err := b.runCommand(b.ExecOnPromote)
				if err != nil {
					log.WithFields(log.Fields{
						"caller": "watch",
						"error":  err,
					}).Error("failed to execute command on promote")
				}
				log.WithFields(log.Fields{
					"caller": "watch",
					"output": out,
				}).Info("command executed on promote")
			}
		} else {
			if b.ExecOnDemote != "" {
				log.WithFields(log.Fields{
					"caller": "watch",
				}).Info("executing command on demote")
				out, err := b.runCommand(b.ExecOnDemote)
				if err != nil {
					log.WithFields(log.Fields{
						"caller": "watch",
						"error":  err,
					}).Error("failed to execute command on demote")
				}
				log.WithFields(log.Fields{
					"caller": "watch",
					"output": out,
				}).Info("command executed on demote")
			}
		}
	}

	if err := wp.RunWithClientAndHclog(b.client, nil); err != nil {
		return err
	}

	return err
}
