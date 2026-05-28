package ballot

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

type ElectionStepStatus string

const (
	ElectionStepLeader         ElectionStepStatus = "leader"
	ElectionStepFollower       ElectionStepStatus = "follower"
	ElectionStepCriticalHealth ElectionStepStatus = "critical_health"
	ElectionStepServiceFailure ElectionStepStatus = "service_failure"
	ElectionStepSessionFailure ElectionStepStatus = "session_failure"
	ElectionStepLockFailure    ElectionStepStatus = "lock_failure"
	ElectionStepCleanupFailure ElectionStepStatus = "cleanup_failure"
	ElectionStepPayloadFailure ElectionStepStatus = "payload_failure"
)

type ElectionStepResult struct {
	Status ElectionStepStatus
	Err    error
	Leader bool
}

type ElectionStep struct {
	ballot *Ballot
}

func NewElectionStep(ballot *Ballot) *ElectionStep {
	return &ElectionStep{ballot: ballot}
}

func (s *ElectionStep) Run() ElectionStepResult {
	b := s.ballot
	if err := b.handleServiceCriticalState(); err != nil {
		return ElectionStepResult{Status: ElectionStepCriticalHealth, Err: err, Leader: false}
	}

	service, _, err := b.getService()
	if err != nil {
		return ElectionStepResult{
			Status: ElectionStepServiceFailure,
			Err:    fmt.Errorf("failed to get service: %w", err),
		}
	}

	if err := b.session(); err != nil {
		return ElectionStepResult{
			Status: ElectionStepSessionFailure,
			Err:    fmt.Errorf("failed to create session: %w", err),
		}
	}

	sessionIDPtr, ok := b.getSessionID()
	if !ok || sessionIDPtr == nil {
		return ElectionStepResult{Status: ElectionStepSessionFailure, Err: fmt.Errorf("session ID is nil")}
	}
	electionPayload := &ElectionPayload{
		Address:   service.Address,
		Port:      service.Port,
		SessionID: *sessionIDPtr,
	}

	if !b.leader.Load() {
		acquired, _, err := b.attemptLeadershipAcquisition(electionPayload)
		if err != nil {
			return ElectionStepResult{
				Status: ElectionStepLockFailure,
				Err:    fmt.Errorf("failed to acquire lock: %w", err),
			}
		}
		if acquired {
			log.WithFields(log.Fields{
				"caller": "election",
			}).Info("Acquired leadership")
		}
	}

	if err := b.verifyAndUpdateLeadershipStatus(); err != nil {
		return ElectionStepResult{Status: ElectionStepPayloadFailure, Err: err, Leader: b.IsLeader()}
	}
	if err := b.cleanup(electionPayload); err != nil {
		return ElectionStepResult{
			Status: ElectionStepCleanupFailure,
			Err:    fmt.Errorf("failed to cleanup: %w", err),
			Leader: b.IsLeader(),
		}
	}
	if b.IsLeader() {
		return ElectionStepResult{Status: ElectionStepLeader, Leader: true}
	}
	return ElectionStepResult{Status: ElectionStepFollower, Leader: false}
}
