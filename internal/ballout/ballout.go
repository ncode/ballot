package ballout

import (
	"fmt"
	election "github.com/dmitriyGarden/consul-leader-election"
	"github.com/hashicorp/consul/api"
	"time"
)

func New(name string, kv string, check string, token string, onPromote string, onDemote string, primaryTag string) (b *Ballout, err error) {
	consulConfig := api.DefaultConfig()
	consulConfig.Token = token
	consul, err := api.NewClient(consulConfig)
	if err != nil {
		return b, err
	}

	b = &Ballout{
		Name:       name,
		KV:         kv,
		Check:      check,
		Token:      token,
		OnPromote:  onPromote,
		OnDemote:   onDemote,
		PrimaryTag: primaryTag,
		Client:     consul,
	}

	return b, err
}

type Ballout struct {
	Name       string
	KV         string
	Check      string
	Token      string
	OnPromote  string
	OnDemote   string
	PrimaryTag string
	Client     *api.Client
}

func (s *Ballout) EventLeader(f bool) {
	if f {
		fmt.Println(s.Name, "I'm the leader!")
	} else {
		fmt.Println(s.Name, "I'm no longer the leader!")
	}
}

func (s *Ballout) Run() {
	electionConfig := &election.ElectionConfig{
		CheckTimeout: 5 * time.Second,
		Client:       s.Client,
		Checks:       []string{s.Check},
		Key:          s.KV,
		LogLevel:     election.LogDebug,
		Event:        s,
	}
	e := election.NewElection(electionConfig)
	// start election
	go e.Init()
	time.Sleep(3 * time.Second)
	if e.IsLeader() {
		fmt.Println("I'm a leader!")
	}
	// to do something ....
	time.Sleep(3 * time.Second)
	if e.IsLeader() {
		fmt.Println("I'm a leader!")
	}
	time.Sleep(3 * time.Second)
	// re-election
	e.ReElection()
	// to do something ....
	time.Sleep(3 * time.Second)
	if e.IsLeader() {
		fmt.Println("I'm a leader!")
	}
	time.Sleep(3 * time.Second)
	// to do something ....
	time.Sleep(3 * time.Second)
	if e.IsLeader() {
		fmt.Println("I'm a leader!")
	}
	// to do something ....
	time.Sleep(3 * time.Second)
	if e.IsLeader() {
		fmt.Println("I'm a leader!")
	}
	// to do something ....
	time.Sleep(3 * time.Second)
	if e.IsLeader() {
		fmt.Println("I'm a leader!")
	}
	e.Stop()
}
