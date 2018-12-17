// Copyright © 2018 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/k-kawa/bqv/bqv"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var projectID string

// applyCmd represents the apply command
var applyCmd = &cobra.Command{
	Use:   "apply",
	Short: "Apply the views",
	Long:  `Apply the views`,
	Run: func(cmd *cobra.Command, args []string) {
		configs, err := bqv.CreateViewConfigsFromDatasetDir(baseDir)
		if err != nil {
			logrus.Errorf("Failed to read views: %s", err.Error())
		}
		ctx := context.Background()

		client, err := bigquery.NewClient(ctx, projectID)
		if err != nil {
			logrus.Panic("Failed to create bigquery client")
		}
		for _, config := range configs {
			if err = config.Apply(ctx, client); err != nil {
				logrus.Printf("Failed to create view %s.%s: %s\n", config.DatasetName, config.ViewName, err.Error())
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(applyCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// applyCmd.PersistentFlags().String("foo", "", "A help for foo")
	applyCmd.PersistentFlags().StringVar(&projectID, "projectID", "", "GCP project name")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// applyCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
