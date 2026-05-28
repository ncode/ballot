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
		case event := <-lifecycle.Events():
			if event.Type == SessionCreated {
				event = <-lifecycle.Events()
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
}
