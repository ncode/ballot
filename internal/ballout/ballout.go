package ballout

import (
	"fmt"
	election "github.com/dmitriyGarden/consul-leader-election"
	"github.com/hashicorp/consul/api"
	"golang.org/x/exp/slices"
	"time"
)

func New(name string, kv string, checks []string, token string, onPromote string, onDemote string, primaryTag string) (b *Ballout, err error) {
	b = &Ballout{
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

type Ballout struct {
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

func (s *Ballout) EventLeader(f bool) {
	agent := s.client.Agent()
	services, err := agent.Services()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%q\n", services)
	service := services[s.ID]
	if f {
		err = agent.ServiceRegister(&api.AgentServiceRegistration{
			Name: s.Name,
			ID:   s.ID,
			Tags: append(service.Tags, s.PrimaryTag),
		})
		fmt.Println(err)
		fmt.Println(s.Name, "I'm the leader!")
	} else {
		tags := service.Tags
		p := slices.Index(tags, s.PrimaryTag)
		if p == -1 {
			return
		}
		err = agent.ServiceRegister(&api.AgentServiceRegistration{
			Name: s.Name,
			ID:   s.ID,
			Tags: slices.Delete(tags, p, p+1),
		})
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(s.Name, "I'm no longer the leader!")
	}
}

func (s *Ballout) Run() error {
	consulConfig := api.DefaultConfig()
	consulConfig.Token = s.Token
	client, err := api.NewClient(consulConfig)
	if err != nil {
		return err
	}
	s.client = client

	electionConfig := &election.ElectionConfig{
		CheckTimeout: 5 * time.Second,
		Client:       s.client,
		Checks:       s.Checks,
		Key:          s.KV,
		LogLevel:     election.LogDebug,
		Event:        s,
	}
	e := election.NewElection(electionConfig)
	e.Init()
	defer e.Stop()

	return nil
}
