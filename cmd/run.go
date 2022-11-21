/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"github.com/ncode/ballot/internal/ballot"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the ballot and starts all the defined elections",
	Run: func(cmd *cobra.Command, args []string) {
		print(viper.GetStringSlice("election.enabled"))
		for _, name := range viper.GetStringSlice("election.enabled") {
			fmt.Println(name)
			b := &ballot.Ballot{}
			err := viper.UnmarshalKey(fmt.Sprintf("election.services.%s", name), b)
			if err != nil {
				panic(err)
			}
			b.Name = name
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
