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
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/ncode/ballot/internal/ballot"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the ballot and starts all the defined elections",
	RunE:  runElection,
}

func runElection(cmd *cobra.Command, args []string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	return runElectionWithContext(ctx)
}

func runElectionWithContext(ctx context.Context) error {
	var wg sync.WaitGroup
	enabledServices := viper.GetStringSlice("election.enabled")

	if len(enabledServices) == 0 {
		return fmt.Errorf("no services enabled for election")
	}

	errCh := make(chan error, len(enabledServices))

	for _, name := range enabledServices {
		b, err := ballot.New(ctx, name)
		if err != nil {
			return fmt.Errorf("failed to create ballot for service %s: %w", name, err)
		}

		wg.Add(1)
		go func(b *ballot.Ballot, name string) {
			defer wg.Done()
			if err := b.Run(); err != nil {
				errCh <- fmt.Errorf("service %s: %w", name, err)
			}
		}(b, name)
	}

	wg.Wait()
	close(errCh)

	// Collect any errors from running elections
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("election errors: %v", errs)
	}

	log.Info("All elections stopped, shutting down")
	return nil
}

func init() {
	rootCmd.AddCommand(runCmd)
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.DebugLevel)
}
