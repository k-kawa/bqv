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
	"fmt"
	"os"
	"strconv"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/k-kawa/bqv/bqv"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// planCmd represents the plan command
var planCmd = &cobra.Command{
	Use:   "plan",
	Short: "Plan shows what's going to happen if you run Apply.",
	Long:  `Plan shows what's going to happen if you run Apply.`,
	Run: func(cmd *cobra.Command, args []string) {
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

		configs, err := bqv.CreateViewConfigsFromDatasetDir(baseDir)
		if err != nil {
			logrus.Errorf("Failed to read views: %s", err.Error())
			os.Exit(1)
		}

		for _, config := range configs {
			diff, err := config.Diff(ctx, client, params)
			if err != nil {
				logrus.Errorf("Failed to create diff of view(%s.%s): %s", config.DatasetName, config.ViewName, err.Error())
				continue
			}
			if diff == nil {
				continue
			}
			queryDiff := "A view query has no change."
			if strings.Compare(diff.OldViewQuery, diff.NewViewQuery) != 0 {
				queryDiff = "### Old\n```sql\n" + diff.OldViewQuery + "\n```\n### New\n```sql\n" + diff.NewViewQuery + "\n```\n"
			}
			fmt.Printf("## %s.%s\n%s\nmetadata update :%s\n",
				diff.DatasetName,
				diff.ViewName,
				queryDiff,
				strconv.FormatBool(diff.MetadataUpdateFlag),
			)
		}
	},
}

func init() {
	rootCmd.AddCommand(planCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// planCmd.PersistentFlags().String("foo", "", "A help for foo")
	planCmd.PersistentFlags().StringVar(&projectID, "projectID", "", "GCP project name")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// planCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
