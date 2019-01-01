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
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/k-kawa/bqv/bqv"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// queryCmd represents the query command
var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Query",
	Long:  ``,
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.New("dataset.view name")
		}
		name := args[0]
		ptn := regexp.MustCompile("^([^.]+)\\.([^.]+)$")
		matched := ptn.MatchString(name)
		if !matched {
			return errors.New("argument must be in (dataset).(view) format")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		configs, err := bqv.CreateViewConfigsFromDatasetDir(baseDir)
		if err != nil {
			logrus.Errorf("Failed to read views: %s", err.Error())
			os.Exit(1)
		}

		names := strings.Split(args[0], ".")

		viewConfig := findViewConfig(configs, names[0], names[1])
		if viewConfig == nil {
			logrus.Error("Not found")
			os.Exit(1)
		}

		params, err := loadParamFile()
		if err != nil {
			logrus.Errorf("%s", err.Error())
			os.Exit(1)
		}

		q, err := viewConfig.QueryWithParam(params)
		if err != nil {
			logrus.Errorf("%s", err.Error())
			os.Exit(1)
		}

		fmt.Print(q)
	},
}

func init() {
	rootCmd.AddCommand(queryCmd)
}

func findViewConfig(viewConfigs []*bqv.ViewConfig, datasetName, viewName string) *bqv.ViewConfig {
	for _, viewConfig := range viewConfigs {
		if strings.Compare(viewConfig.DatasetName, datasetName) == 0 && strings.Compare(viewConfig.ViewName, viewName) == 0 {
			return viewConfig
		}
	}
	return nil
}
