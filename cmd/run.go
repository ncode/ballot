/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"github.com/ncode/ballot/internal/ballout"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the ballot and starts all the defined elections",
	Run: func(cmd *cobra.Command, args []string) {
		print(viper.GetStringSlice("election.enabled"))
		for _, name := range viper.GetStringSlice("election.enabled") {
			fmt.Println(name)
			b := &ballout.Ballot{}
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
}
