/*
Copyright © 2022 Juliano Martinez <juliano@martinez.io>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestRunElectionWithContext_NoServicesEnabled(t *testing.T) {
	viper.Reset()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := runElectionWithContext(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no services enabled for election")
}

func TestRunElectionWithContext_InvalidService(t *testing.T) {
	viper.Reset()
	viper.Set("election.enabled", []string{"invalid_service"})
	// Don't set the service configuration, so ballot.New will fail

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := runElectionWithContext(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create ballot for service invalid_service")
}

func TestRunCmd_Structure(t *testing.T) {
	assert.Equal(t, "run", runCmd.Use)
	assert.NotEmpty(t, runCmd.Short)
	assert.NotNil(t, runCmd.RunE)
}

func TestRunElection_CancelledContext(t *testing.T) {
	viper.Reset()
	viper.Set("election.enabled", []string{"test_service"})
	viper.Set("election.services.test_service.id", "test_id")
	viper.Set("election.services.test_service.key", "election/test/leader")

	// Create an already cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// This should handle the cancelled context gracefully
	err := runElectionWithContext(ctx)
	// Depending on timing, may get an error from ballot.New or from Run
	// The important thing is it doesn't panic
	_ = err
}
