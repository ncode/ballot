package ballot

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestSessionLifecycle(t *testing.T) {
	runtimeConfig := RuntimeConfig{
		Name:          "test_service",
		ID:            "test_service_id",
		Key:           "election/test/leader",
		ServiceChecks: []string{"service:test_service_id"},
		TTL:           10 * time.Second,
		LockDelay:     3 * time.Second,
	}

	t.Run("creates session with checks and timing", func(t *testing.T) {
		var sessionID atomic.Value
		mockSession := new(MockSession)
		mockSession.On("Create", mock.MatchedBy(func(entry *api.SessionEntry) bool {
			return entry.Behavior == "delete" &&
				assert.ObjectsAreEqual([]string{"service:test_service_id", "serfHealth"}, entry.Checks) &&
				entry.TTL == "10s" &&
				entry.LockDelay == 3*time.Second
		}), (*api.WriteOptions)(nil)).Return("session-1", nil, nil)
		mockSession.On("RenewPeriodic", "10s", "session-1", (*api.WriteOptions)(nil), mock.Anything).Return(nil)

		lifecycle := NewSessionLifecycle(context.Background(), mockSession, &sessionID, runtimeConfig)
		id, err := lifecycle.ActiveSession()

		require.NoError(t, err)
		assert.Equal(t, "session-1", id)
		stored, ok := lifecycle.getSessionID()
		require.True(t, ok)
		require.NotNil(t, stored)
		assert.Equal(t, "session-1", *stored)
	})

	t.Run("reuses valid session", func(t *testing.T) {
		var sessionID atomic.Value
		current := "session-1"
		sessionID.Store(&current)
		mockSession := new(MockSession)
		mockSession.On("Info", current, (*api.QueryOptions)(nil)).Return(&api.SessionEntry{ID: current}, &api.QueryMeta{}, nil)

		lifecycle := NewSessionLifecycle(context.Background(), mockSession, &sessionID, runtimeConfig)
		id, err := lifecycle.ActiveSession()

		require.NoError(t, err)
		assert.Equal(t, current, id)
		mockSession.AssertNotCalled(t, "Create", mock.Anything, mock.Anything)
	})

	t.Run("replaces invalid session", func(t *testing.T) {
		var sessionID atomic.Value
		current := "session-1"
		sessionID.Store(&current)
		mockSession := new(MockSession)
		mockSession.On("Info", current, (*api.QueryOptions)(nil)).Return((*api.SessionEntry)(nil), &api.QueryMeta{}, nil)
		mockSession.On("Create", mock.Anything, (*api.WriteOptions)(nil)).Return("session-2", nil, nil)
		mockSession.On("RenewPeriodic", "10s", "session-2", (*api.WriteOptions)(nil), mock.Anything).Return(nil)

		lifecycle := NewSessionLifecycle(context.Background(), mockSession, &sessionID, runtimeConfig)
		id, err := lifecycle.ActiveSession()

		require.NoError(t, err)
		assert.Equal(t, "session-2", id)
	})

	t.Run("renewal failure is observable", func(t *testing.T) {
		var sessionID atomic.Value
		renewErr := errors.New("renew failed")
		mockSession := new(MockSession)
		mockSession.On("Create", mock.Anything, (*api.WriteOptions)(nil)).Return("session-1", nil, nil)
		mockSession.On("RenewPeriodic", "10s", "session-1", (*api.WriteOptions)(nil), mock.Anything).Return(renewErr)

		lifecycle := NewSessionLifecycle(context.Background(), mockSession, &sessionID, runtimeConfig)
		_, err := lifecycle.ActiveSession()
		require.NoError(t, err)

		select {
		case event := <-lifecycle.events:
			if event.Type == SessionCreated {
				event = <-lifecycle.events
			}
			assert.Equal(t, SessionRenewalFailed, event.Type)
			assert.Equal(t, renewErr, event.Err)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for renewal failure")
		}
		stored, ok := lifecycle.getSessionID()
		require.True(t, ok)
		assert.Nil(t, stored)
	})

	t.Run("release destroys existing session", func(t *testing.T) {
		var sessionID atomic.Value
		current := "session-1"
		sessionID.Store(&current)
		mockSession := new(MockSession)
		mockSession.On("Destroy", current, (*api.WriteOptions)(nil)).Return(&api.WriteMeta{}, nil)

		lifecycle := NewSessionLifecycle(context.Background(), mockSession, &sessionID, runtimeConfig)
		err := lifecycle.Release()

		require.NoError(t, err)
		stored, ok := lifecycle.getSessionID()
		require.True(t, ok)
		assert.Nil(t, stored)
	})

	t.Run("nil context defaults to background and clones checks", func(t *testing.T) {
		var sessionID atomic.Value
		cfg := runtimeConfig
		cfg.ServiceChecks = []string{"service:test_service_id"}

		lifecycle := NewSessionLifecycle(nil, nil, &sessionID, cfg)
		cfg.ServiceChecks[0] = "changed"

		require.NotNil(t, lifecycle.ctx)
		assert.Equal(t, []string{"service:test_service_id"}, lifecycle.checks)
	})

	t.Run("active session without consul session client returns error", func(t *testing.T) {
		var sessionID atomic.Value
		lifecycle := NewSessionLifecycle(context.Background(), nil, &sessionID, runtimeConfig)

		id, err := lifecycle.ActiveSession()

		require.Error(t, err)
		assert.Empty(t, id)
		assert.Contains(t, err.Error(), "consul client is required")
	})

	t.Run("release without session is no-op", func(t *testing.T) {
		var sessionID atomic.Value
		mockSession := new(MockSession)
		lifecycle := NewSessionLifecycle(context.Background(), mockSession, &sessionID, runtimeConfig)

		err := lifecycle.Release()

		require.NoError(t, err)
		mockSession.AssertNotCalled(t, "Destroy", mock.Anything, mock.Anything)
	})

	t.Run("renewal panic is recovered", func(t *testing.T) {
		var sessionID atomic.Value
		renewStarted := make(chan struct{})
		session := &panicRenewSession{renewStarted: renewStarted}

		lifecycle := NewSessionLifecycle(context.Background(), session, &sessionID, runtimeConfig)
		id, err := lifecycle.ActiveSession()

		require.NoError(t, err)
		assert.Equal(t, "session-1", id)
		select {
		case <-renewStarted:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for renewal")
		}
	})

	t.Run("publish drops events when buffer is full", func(t *testing.T) {
		var sessionID atomic.Value
		lifecycle := NewSessionLifecycle(context.Background(), nil, &sessionID, runtimeConfig)

		for i := 0; i < cap(lifecycle.events); i++ {
			lifecycle.publish(SessionEvent{Type: SessionCreated})
		}
		lifecycle.publish(SessionEvent{Type: SessionReleased})

		assert.Equal(t, cap(lifecycle.events), len(lifecycle.events))
	})
}

type panicRenewSession struct {
	renewStarted chan struct{}
}

func (s *panicRenewSession) Create(*api.SessionEntry, *api.WriteOptions) (string, *api.WriteMeta, error) {
	return "session-1", nil, nil
}

func (s *panicRenewSession) Destroy(string, *api.WriteOptions) (*api.WriteMeta, error) {
	return nil, nil
}

func (s *panicRenewSession) Info(string, *api.QueryOptions) (*api.SessionEntry, *api.QueryMeta, error) {
	return nil, nil, nil
}

func (s *panicRenewSession) RenewPeriodic(string, string, *api.WriteOptions, <-chan struct{}) error {
	close(s.renewStarted)
	panic("renew panic")
}
