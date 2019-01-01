// Copyright © 2019 Kohei Kawasaki <mynameiskawasaq@gmail.com>
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
	"os"

	"cloud.google.com/go/bigquery"
	"github.com/k-kawa/bqv/bqv"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var dryRun bool

// applyCmd represents the apply command
var applyCmd = &cobra.Command{
	Use:   "apply",
	Short: "Apply builds and updates thew views you defined.",
	Long:  `Apply builds and updates thew views you defined.`,
	Run: func(cmd *cobra.Command, args []string) {
		configs, err := bqv.CreateViewConfigsFromDatasetDir(baseDir)
		if err != nil {
			logrus.Errorf("Failed to read views: %s", err.Error())
			os.Exit(1)
		}

		params, err := loadParamFile()
		if err != nil {
			logrus.Errorf("Failed to read parameteer file: %s", err.Error())
			os.Exit(1)
		}

		ctx := context.Background()

		client, err := bigquery.NewClient(ctx, projectID)
		if err != nil {
			logrus.Errorf("Failed to create bigquery client: %s", err.Error())
			os.Exit(1)
		}

		errCount := 0
		if dryRun {
			for _, config := range configs {
				if _, err = config.DryRun(ctx, client, params); err != nil {
					logrus.Errorf("Failed to create view %s.%s (dry-run): %s", config.DatasetName, config.ViewName, err.Error())
					errCount++
				}
			}
		} else {
			for _, config := range configs {
				if _, err = config.Apply(ctx, client, params); err != nil {
					logrus.Errorf("Failed to create view %s.%s: %s", config.DatasetName, config.ViewName, err.Error())
					errCount++
				}
			}
		}

		if errCount > 0 {
			logrus.Errorf("%d errors occured", errCount)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(applyCmd)

	applyCmd.PersistentFlags().StringVar(&projectID, "projectID", "", "GCP project name")
	applyCmd.PersistentFlags().BoolVar(&dryRun, "dry-run", false, "Dry run")
}
