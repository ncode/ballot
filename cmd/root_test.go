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
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestRootCmd_Structure(t *testing.T) {
	assert.Equal(t, "ballot", rootCmd.Use)
	assert.NotEmpty(t, rootCmd.Short)
	assert.NotEmpty(t, rootCmd.Long)
}

func TestRootCmd_HasRunSubcommand(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "run" {
			found = true
			break
		}
	}
	assert.True(t, found, "rootCmd should have 'run' subcommand")
}

func TestRootCmd_ExecuteHelp(t *testing.T) {
	// Capture output
	buf := new(bytes.Buffer)
	rootCmd.SetOut(buf)
	rootCmd.SetErr(buf)
	rootCmd.SetArgs([]string{"--help"})

	err := rootCmd.Execute()
	assert.NoError(t, err)
	assert.Contains(t, buf.String(), "ballot")
}

func TestInitConfig_WithConfigFile(t *testing.T) {
	// Create a temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "test-config.yaml")
	configContent := []byte(`
election:
  enabled:
    - test_service
`)
	err := os.WriteFile(configPath, configContent, 0644)
	assert.NoError(t, err)

	// Reset viper and set config file
	viper.Reset()
	cfgFile = configPath

	// Call initConfig
	initConfig()

	// Verify the config was loaded
	enabled := viper.GetStringSlice("election.enabled")
	assert.Contains(t, enabled, "test_service")

	// Clean up
	cfgFile = ""
	viper.Reset()
}

func TestInitConfig_WithoutConfigFile(t *testing.T) {
	// Reset viper
	viper.Reset()
	cfgFile = ""

	// This should not panic even without a config file
	initConfig()
}

func TestRootCmd_PersistentFlags(t *testing.T) {
	flag := rootCmd.PersistentFlags().Lookup("config")
	assert.NotNil(t, flag)
	assert.Equal(t, "config", flag.Name)
}
