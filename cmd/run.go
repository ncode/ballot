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
	"fmt"
	"os"
	"time"

	"github.com/ncode/ballot/internal/ballot"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the ballot and starts all the defined elections",
	Run: func(cmd *cobra.Command, args []string) {
		for _, name := range viper.GetStringSlice("election.enabled") {
			b := &ballot.Ballot{}
			err := viper.UnmarshalKey(fmt.Sprintf("election.services.%s", name), b)
			if err != nil {
				panic(err)
			}
			b.Name = name
			if b.LockDelay == 0 {
				b.LockDelay = 3 * time.Second
			}
			if b.TTL == 0 {
				b.TTL = 10 * time.Second
			}
			err = b.Run()
			if err != nil {
				log.WithFields(log.Fields{
					"caller": "run",
					"step":   "runCmd",
				}).Error(err)
			}
		}
	},
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
