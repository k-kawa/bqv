// Copyright © 2018 Kohei Kawasaki <mynameiskawasaq@gmail.com>
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

// destroyCmd represents the destroy command
var destroyCmd = &cobra.Command{
	Use:   "destroy",
	Short: "Destroy the views",
	Long:  `Destroy the views`,
	Run: func(cmd *cobra.Command, args []string) {
		configs, err := bqv.CreateViewConfigsFromDatasetDir(baseDir)
		if err != nil {
			logrus.Errorf("Failed to read views: %s", err.Error())
			os.Exit(1)
		}
		ctx := context.Background()
		client, err := bigquery.NewClient(ctx, projectID)
		if err != nil {
			logrus.Errorf("Failed to create bigquery client: %s", err.Error())
			os.Exit(1)
		}
		errCount := 0
		for _, config := range configs {
			if _, err = config.DeleteIfExist(ctx, client); err != nil {
				logrus.Errorf("Failed to delete a view %s.%s: %s", config.DatasetName, config.ViewName, err.Error())
				errCount += 1
			} else {
				logrus.Printf("Deleting view %s.%s", config.DatasetName, config.ViewName)
			}
		}
		if errCount > 0 {
			logrus.Errorf("Some views might get deleted but %d errors occured", errCount)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(destroyCmd)
	destroyCmd.PersistentFlags().StringVar(&projectID, "projectID", "", "GCP project name")
}
