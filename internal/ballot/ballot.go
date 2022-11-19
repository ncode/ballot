package ballot

import (
	election "github.com/dmitriyGarden/consul-leader-election"
	"github.com/hashicorp/consul/api"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
	"os/exec"
	"time"
)

// New returns a new Ballot instance.
func New(name string, kv string, checks []string, token string, onPromote string, onDemote string, primaryTag string) (b *Ballot, err error) {
	b = &Ballot{
		Name:       name,
		KV:         kv,
		Checks:     checks,
		Token:      token,
		OnPromote:  onPromote,
		OnDemote:   onDemote,
		PrimaryTag: primaryTag,
		client:     nil,
	}

	return b, err
}

// Ballot is a struct that holds the configuration for the leader election.
type Ballot struct {
	Name       string      `mapstructure:"-"`
	ID         string      `mapstructure:"id"`
	KV         string      `mapstructure:"kv"`
	Checks     []string    `mapstructure:"checks"`
	Token      string      `mapstructure:"token"`
	OnPromote  string      `mapstructure:"onPromote"`
	OnDemote   string      `mapstructure:"onDemote"`
	PrimaryTag string      `mapstructure:"primaryTag"`
	client     *api.Client `mapstructure:"-"`
}

// runCommand runs a command and returns the output.
func (s *Ballot) runCommand(command string) ([]byte, error) {
	log.WithFields(log.Fields{
		"caller": "runCommand",
	}).Info("Running command: ", command)
	cmd := exec.Command(command)
	return cmd.Output()
}

// Copy *api.AgentService to *api.AgentServiceRegistration
func (s *Ballot) copyServiceToRegistration(service *api.AgentService) *api.AgentServiceRegistration {
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

// EventLeader is called when the leader status changes.
func (s *Ballot) EventLeader(f bool) {
	agent := s.client.Agent()
	services, err := agent.Services()
	if err != nil {
		log.WithFields(log.Fields{
			"caller": "EventLeader",
			"step":   "get services",
		}).Error(err)
	}

	service := services[s.ID]
	if f {
		log.WithFields(log.Fields{
			"state": "follower",
			"step":  "EventLeader",
		}).Info("Adding primary tag to service")

		registration := s.copyServiceToRegistration(service)
		registration.Tags = append(registration.Tags, s.PrimaryTag)
		err = agent.ServiceRegister(registration)
		if err != nil {
			log.WithFields(log.Fields{
				"state":  "leader",
				"caller": "EventLeader",
				"step":   "update service",
			}).Error(err)
		}

		if s.OnPromote != "" {
			out, err := s.runCommand(s.OnPromote)
			if err != nil {
				log.WithFields(log.Fields{
					"state":  "leader",
					"caller": "EventLeader",
					"step":   "onPromote",
				}).Error(err)
				return
			}
			log.WithFields(log.Fields{
				"state":  "leader",
				"caller": "EventLeader",
				"step":   "onPromote",
			}).Info(string(out))
		}
	} else {
		log.WithFields(log.Fields{
			"state": "follower",
			"step":  "EventLeader",
		}).Info("Removing primary tag from service")

		tags := service.Tags
		p := slices.Index(tags, s.PrimaryTag)
		if p == -1 {
			return
		}
		registration := s.copyServiceToRegistration(service)
		registration.Tags = tags
		err = agent.ServiceRegister(registration)
		if err != nil {
			log.WithFields(log.Fields{
				"state":  "follower",
				"caller": "EventLeader",
				"step":   "update service",
			}).Error(err)
		}

		if s.OnDemote != "" {
			out, err := s.runCommand(s.OnDemote)
			if err != nil {
				log.WithFields(log.Fields{
					"state":  "follower",
					"caller": "EventLeader",
					"step":   "onDemote",
				}).Error(err)
				return
			}
			log.WithFields(log.Fields{
				"state":  "follower",
				"caller": "EventLeader",
				"step":   "onDemote",
			}).Info(string(out))
		}
	}
}

// Run starts the leader election.
func (s *Ballot) Run() error {
	consulConfig := api.DefaultConfig()
	consulConfig.Token = s.Token
	client, err := api.NewClient(consulConfig)
	if err != nil {
		return err
	}
	s.client = client

	// TODO: Send a PR upstream to allow to pass a logger to the leader election.
	electionConfig := &election.ElectionConfig{
		CheckTimeout: 5 * time.Second,
		Client:       s.client,
		Checks:       s.Checks,
		Key:          s.KV,
		LogLevel:     election.LogError,
		Event:        s,
	}
	e := election.NewElection(electionConfig)
	e.Init()
	defer e.Stop()

	return nil
}
