package ballot

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLeadershipHooks_Execute(t *testing.T) {
	payload := &ElectionPayload{
		Address:   "127.0.0.1",
		Port:      8080,
		SessionID: "abc123",
	}

	t.Run("promotion hook completes with environment", func(t *testing.T) {
		hooks := NewLeadershipHooks(&commandExecutor{})

		result := hooks.Execute(context.Background(), HookRequest{
			Transition: HookPromote,
			Command:    `sh -c 'printf "%s:%s:%s" "$ADDRESS" "$PORT" "$SESSIONID"'`,
			Payload:    payload,
			Timeout:    time.Second,
		})

		require.NoError(t, result.Err)
		assert.Equal(t, HookPromote, result.Transition)
		assert.Equal(t, "127.0.0.1:8080:abc123", string(result.Output))
		assert.False(t, result.TimedOut)
	})

	t.Run("demotion hook completes", func(t *testing.T) {
		hooks := NewLeadershipHooks(&commandExecutor{})

		result := hooks.Execute(context.Background(), HookRequest{
			Transition: HookDemote,
			Command:    `sh -c 'printf demoted'`,
			Payload:    payload,
			Timeout:    time.Second,
		})

		require.NoError(t, result.Err)
		assert.Equal(t, HookDemote, result.Transition)
		assert.Equal(t, "demoted", string(result.Output))
	})

	t.Run("command failure is observable", func(t *testing.T) {
		hooks := NewLeadershipHooks(&commandExecutor{})

		result := hooks.Execute(context.Background(), HookRequest{
			Command: `sh -c 'exit 7'`,
			Payload: payload,
			Timeout: time.Second,
		})

		require.Error(t, result.Err)
		assert.False(t, result.TimedOut)
	})

	t.Run("timeout is observable", func(t *testing.T) {
		hooks := NewLeadershipHooks(&commandExecutor{})

		result := hooks.Execute(context.Background(), HookRequest{
			Command: `sh -c 'sleep 1'`,
			Payload: payload,
			Timeout: 10 * time.Millisecond,
		})

		require.Error(t, result.Err)
		assert.True(t, result.TimedOut)
	})

	t.Run("no hook configured", func(t *testing.T) {
		hooks := NewLeadershipHooks(&commandExecutor{})

		result := hooks.Execute(context.Background(), HookRequest{Payload: payload})

		assert.NoError(t, result.Err)
		assert.True(t, result.Skipped)
	})

	t.Run("nil executor uses command executor", func(t *testing.T) {
		hooks := NewLeadershipHooks(nil)

		result := hooks.Execute(context.Background(), HookRequest{
			Command: `sh -c 'printf default-executor'`,
			Payload: payload,
			Timeout: time.Second,
		})

		require.NoError(t, result.Err)
		assert.Equal(t, "default-executor", string(result.Output))
	})

	t.Run("missing payload fails before command execution", func(t *testing.T) {
		hooks := NewLeadershipHooks(&commandExecutor{})

		result := hooks.Execute(context.Background(), HookRequest{
			Command: "echo should-not-run",
		})

		require.Error(t, result.Err)
		assert.Contains(t, result.Err.Error(), "hook payload is required")
	})

	t.Run("malformed command fails before command execution", func(t *testing.T) {
		hooks := NewLeadershipHooks(&commandExecutor{})

		result := hooks.Execute(context.Background(), HookRequest{
			Command: "'unterminated",
			Payload: payload,
		})

		require.Error(t, result.Err)
	})

	t.Run("comment-only command is rejected as empty", func(t *testing.T) {
		hooks := NewLeadershipHooks(&commandExecutor{})

		result := hooks.Execute(context.Background(), HookRequest{
			Command: "# skipped by shlex",
			Payload: payload,
		})

		require.Error(t, result.Err)
		assert.Contains(t, result.Err.Error(), "empty command")
	})
}

func TestBallot_ObserveHookResult(t *testing.T) {
	t.Run("observes published result", func(t *testing.T) {
		b := &Ballot{}
		expected := HookResult{Transition: HookPromote, Command: "echo promoted", Output: []byte("promoted\n")}

		b.publishHookResult(expected)
		result, ok := observeHookResult(t, b)

		require.True(t, ok)
		assert.Equal(t, expected.Transition, result.Transition)
		assert.Equal(t, expected.Command, result.Command)
		assert.Equal(t, expected.Output, result.Output)
	})

	t.Run("returns false when context is cancelled", func(t *testing.T) {
		b := &Ballot{}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		result, ok := b.ObserveHookResult(ctx)

		assert.False(t, ok)
		assert.ErrorIs(t, result.Err, context.Canceled)
	})

	t.Run("concurrent publish and observe share one channel", func(t *testing.T) {
		b := &Ballot{}
		const count = 8

		var publishers sync.WaitGroup
		for i := 0; i < count; i++ {
			publishers.Add(1)
			go func() {
				defer publishers.Done()
				b.publishHookResult(HookResult{Transition: HookPromote})
			}()
		}
		publishers.Wait()

		for i := 0; i < count; i++ {
			result, ok := observeHookResult(t, b)
			require.True(t, ok)
			assert.Equal(t, HookPromote, result.Transition)
		}
	})
}
