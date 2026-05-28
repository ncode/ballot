package ballot

import (
	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/mock"
)

type MockConsulClient struct {
	mock.Mock
}

func (m *MockConsulClient) Agent() AgentInterface {
	args := m.Called()
	return args.Get(0).(AgentInterface)
}

func (m *MockConsulClient) Catalog() CatalogInterface {
	args := m.Called()
	return args.Get(0).(CatalogInterface)
}

func (m *MockConsulClient) Health() HealthInterface {
	args := m.Called()
	return args.Get(0).(HealthInterface)
}

func (m *MockConsulClient) KV() KVInterface {
	args := m.Called()
	return args.Get(0).(KVInterface)
}

func (m *MockConsulClient) Session() SessionInterface {
	args := m.Called()
	return args.Get(0).(SessionInterface)
}

type MockAgent struct {
	mock.Mock
}

func (m *MockAgent) Service(serviceID string, q *api.QueryOptions) (*api.AgentService, *api.QueryMeta, error) {
	args := m.Called(serviceID, q)
	var service *api.AgentService
	if args.Get(0) != nil {
		service = args.Get(0).(*api.AgentService)
	}
	var meta *api.QueryMeta
	if args.Get(1) != nil {
		meta = args.Get(1).(*api.QueryMeta)
	}
	return service, meta, args.Error(2)
}

func (m *MockAgent) ServiceRegister(service *api.AgentServiceRegistration) error {
	args := m.Called(service)
	return args.Error(0)
}

func (m *MockAgent) ServiceDeregister(serviceID string) error {
	args := m.Called(serviceID)
	return args.Error(0)
}

type MockCatalog struct {
	mock.Mock
}

func (m *MockCatalog) Service(serviceName, tag string, q *api.QueryOptions) ([]*api.CatalogService, *api.QueryMeta, error) {
	args := m.Called(serviceName, tag, q)
	var services []*api.CatalogService
	if args.Get(0) != nil {
		services = args.Get(0).([]*api.CatalogService)
	}
	var meta *api.QueryMeta
	if args.Get(1) != nil {
		meta = args.Get(1).(*api.QueryMeta)
	}
	return services, meta, args.Error(2)
}

func (m *MockCatalog) Register(reg *api.CatalogRegistration, w *api.WriteOptions) (*api.WriteMeta, error) {
	args := m.Called(reg, w)
	var meta *api.WriteMeta
	if args.Get(0) != nil {
		meta = args.Get(0).(*api.WriteMeta)
	}
	return meta, args.Error(1)
}

func (m *MockCatalog) Deregister(dereg *api.CatalogDeregistration, w *api.WriteOptions) (*api.WriteMeta, error) {
	args := m.Called(dereg, w)
	var meta *api.WriteMeta
	if args.Get(0) != nil {
		meta = args.Get(0).(*api.WriteMeta)
	}
	return meta, args.Error(1)
}

type MockSession struct {
	mock.Mock
}

func (m *MockSession) Create(se *api.SessionEntry, q *api.WriteOptions) (string, *api.WriteMeta, error) {
	args := m.Called(se, q)
	sessionID := args.String(0)
	var meta *api.WriteMeta
	if args.Get(1) != nil {
		meta = args.Get(1).(*api.WriteMeta)
	}
	return sessionID, meta, args.Error(2)
}

func (m *MockSession) Destroy(sessionID string, q *api.WriteOptions) (*api.WriteMeta, error) {
	args := m.Called(sessionID, q)
	var meta *api.WriteMeta
	if args.Get(0) != nil {
		meta = args.Get(0).(*api.WriteMeta)
	}
	return meta, args.Error(1)
}

func (m *MockSession) Info(sessionID string, q *api.QueryOptions) (*api.SessionEntry, *api.QueryMeta, error) {
	args := m.Called(sessionID, q)
	var entry *api.SessionEntry
	if args.Get(0) != nil {
		entry = args.Get(0).(*api.SessionEntry)
	}
	var meta *api.QueryMeta
	if args.Get(1) != nil {
		meta = args.Get(1).(*api.QueryMeta)
	}
	return entry, meta, args.Error(2)
}

func (m *MockSession) RenewPeriodic(initialTTL string, id string, q *api.WriteOptions, doneCh <-chan struct{}) error {
	args := m.Called(initialTTL, id, q, doneCh)
	return args.Error(0)
}

type MockHealth struct {
	mock.Mock
}

func (m *MockHealth) Checks(service string, q *api.QueryOptions) (api.HealthChecks, *api.QueryMeta, error) {
	args := m.Called(service, q)
	var checks api.HealthChecks
	switch value := args.Get(0).(type) {
	case api.HealthChecks:
		checks = value
	case []*api.HealthCheck:
		checks = api.HealthChecks(value)
	}
	meta, _ := args.Get(1).(*api.QueryMeta)
	return checks, meta, args.Error(2)
}

type MockKV struct {
	mock.Mock
}

func (m *MockKV) Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error) {
	args := m.Called(key, q)
	pair, _ := args.Get(0).(*api.KVPair)
	meta, _ := args.Get(1).(*api.QueryMeta)
	return pair, meta, args.Error(2)
}

func (m *MockKV) Put(p *api.KVPair, q *api.WriteOptions) (*api.WriteMeta, error) {
	args := m.Called(p, q)
	meta, _ := args.Get(0).(*api.WriteMeta)
	return meta, args.Error(1)
}

func (m *MockKV) Acquire(p *api.KVPair, q *api.WriteOptions) (bool, *api.WriteMeta, error) {
	args := m.Called(p, q)
	boolResult := args.Bool(0)
	var meta *api.WriteMeta
	if args.Get(1) != nil {
		meta = args.Get(1).(*api.WriteMeta)
	}
	return boolResult, meta, args.Error(2)
}
