package ballot

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/consul/api"
	log "github.com/sirupsen/logrus"
)

type SessionEventType string

const (
	SessionCreated       SessionEventType = "created"
	SessionRenewalFailed SessionEventType = "renewal_failed"
	SessionCancelled     SessionEventType = "cancelled"
	SessionReleased      SessionEventType = "released"
)

type SessionEvent struct {
	Type      SessionEventType
	SessionID string
	Err       error
}

type SessionLifecycle struct {
	ctx         context.Context
	session     SessionInterface
	sessionID   *atomic.Value
	checks      []string
	ttl         string
	lockDelay   time.Duration
	cancelMu    sync.Mutex
	renewCancel context.CancelFunc
	events      chan SessionEvent
}

func NewSessionLifecycle(ctx context.Context, session SessionInterface, sessionID *atomic.Value, cfg RuntimeConfig) *SessionLifecycle {
	if ctx == nil {
		ctx = context.Background()
	}
	return &SessionLifecycle{
		ctx:       ctx,
		session:   session,
		sessionID: sessionID,
		checks:    slices.Clone(cfg.ServiceChecks),
		ttl:       cfg.TTL.String(),
		lockDelay: cfg.LockDelay,
		events:    make(chan SessionEvent, 8),
	}
}

func (sl *SessionLifecycle) ActiveSession() (string, error) {
	if sl.session == nil {
		return "", fmt.Errorf("consul client is required")
	}
	if current, ok := sl.currentSessionID(); ok && current != "" {
		sessionInfo, _, err := sl.session.Info(current, nil)
		if err != nil {
			return "", err
		}
		if sessionInfo != nil {
			log.WithFields(log.Fields{
				"caller":  "session",
				"session": current,
			}).Trace("Returning cached session")
			return current, nil
		}
		sl.cancelRenewal(SessionCancelled, current, nil)
	}

	log.WithFields(log.Fields{
		"caller": "session",
	}).Trace("Creating new session")
	sessionID, _, err := sl.session.Create(&api.SessionEntry{
		Behavior:  "delete",
		Checks:    append(slices.Clone(sl.checks), "serfHealth"),
		TTL:       sl.ttl,
		LockDelay: sl.lockDelay,
	}, nil)
	if err != nil {
		return "", err
	}

	log.WithFields(log.Fields{
		"caller": "session",
		"ID":     sessionID,
	}).Trace("Storing session ID")
	sl.sessionID.Store(&sessionID)
	sl.publish(SessionEvent{Type: SessionCreated, SessionID: sessionID})
	sl.startRenewal(sessionID)
	return sessionID, nil
}

func (sl *SessionLifecycle) Release() error {
	sessionIDPtr, ok := sl.getSessionID()
	if !ok || sessionIDPtr == nil {
		return nil
	}
	sessionID := *sessionIDPtr
	sl.sessionID.Store((*string)(nil))
	sl.cancelRenewal(SessionReleased, sessionID, nil)
	_, err := sl.session.Destroy(sessionID, nil)
	return err
}

func (sl *SessionLifecycle) Events() <-chan SessionEvent {
	return sl.events
}

func (sl *SessionLifecycle) getSessionID() (*string, bool) {
	sessionIDValue := sl.sessionID.Load()
	if sessionIDValue == nil {
		return nil, false
	}
	sessionIDPtr, ok := sessionIDValue.(*string)
	return sessionIDPtr, ok
}

func (sl *SessionLifecycle) currentSessionID() (string, bool) {
	sessionIDPtr, ok := sl.getSessionID()
	if !ok || sessionIDPtr == nil {
		return "", false
	}
	return *sessionIDPtr, true
}

func (sl *SessionLifecycle) startRenewal(sessionID string) {
	sl.cancelRenewal(SessionCancelled, sessionID, nil)

	renewCtx, renewCancel := context.WithCancel(sl.ctx)
	sl.cancelMu.Lock()
	sl.renewCancel = renewCancel
	sl.cancelMu.Unlock()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.WithFields(log.Fields{
					"caller": "session",
					"error":  r,
				}).Error("Recovered from panic in session renewal")
			}
		}()

		err := sl.session.RenewPeriodic(sl.ttl, sessionID, nil, renewCtx.Done())
		if err != nil {
			log.WithFields(log.Fields{
				"caller": "session",
				"error":  err,
			}).Warning("Failed to renew session")
			sl.sessionID.Store((*string)(nil))
			sl.publish(SessionEvent{Type: SessionRenewalFailed, SessionID: sessionID, Err: err})
		}
	}()
}

func (sl *SessionLifecycle) cancelRenewal(eventType SessionEventType, sessionID string, err error) {
	sl.cancelMu.Lock()
	cancel := sl.renewCancel
	sl.renewCancel = nil
	sl.cancelMu.Unlock()
	if cancel != nil {
		cancel()
		sl.publish(SessionEvent{Type: eventType, SessionID: sessionID, Err: err})
	}
}

func (sl *SessionLifecycle) publish(event SessionEvent) {
	select {
	case sl.events <- event:
	default:
	}
}
