package ballot

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/shlex"
)

type HookTransition string

const (
	HookPromote HookTransition = "promote"
	HookDemote  HookTransition = "demote"
)

type HookRequest struct {
	Transition HookTransition
	Command    string
	Payload    *ElectionPayload
	Timeout    time.Duration
}

type HookResult struct {
	Transition HookTransition
	Command    string
	Output     []byte
	Err        error
	TimedOut   bool
	Skipped    bool
}

type LeadershipHooks struct {
	executor CommandExecutor
}

func NewLeadershipHooks(executor CommandExecutor) *LeadershipHooks {
	if executor == nil {
		executor = &commandExecutor{}
	}
	return &LeadershipHooks{executor: executor}
}

func (h *LeadershipHooks) Execute(ctx context.Context, req HookRequest) HookResult {
	result := HookResult{
		Transition: req.Transition,
		Command:    req.Command,
	}
	if strings.TrimSpace(req.Command) == "" {
		result.Skipped = true
		return result
	}
	if req.Payload == nil {
		result.Err = fmt.Errorf("hook payload is required")
		return result
	}

	args, err := shlex.Split(req.Command)
	if err != nil {
		result.Err = err
		return result
	}
	if len(args) == 0 {
		result.Err = fmt.Errorf("empty command")
		return result
	}

	commandCtx := ctx
	cancel := func() {}
	if req.Timeout > 0 {
		commandCtx, cancel = context.WithTimeout(ctx, req.Timeout)
	}
	defer cancel()

	cmd := h.executor.CommandContext(commandCtx, args[0], args[1:]...)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("ADDRESS=%s", req.Payload.Address),
		fmt.Sprintf("PORT=%d", req.Payload.Port),
		fmt.Sprintf("SESSIONID=%s", req.Payload.SessionID),
	)

	result.Output, result.Err = cmd.CombinedOutput()
	result.TimedOut = commandCtx.Err() == context.DeadlineExceeded
	return result
}

func HookTimeout(ttl, lockDelay time.Duration) time.Duration {
	timeout := (ttl + lockDelay) * 2
	if timeout <= 0 {
		return DefaultSessionTTL
	}
	return timeout
}
