package bqv

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"text/template"

	"google.golang.org/api/googleapi"

	"github.com/sirupsen/logrus"

	"cloud.google.com/go/bigquery"
)

type ViewConfig struct {
	Query       string
	ViewName    string
	DatasetName string
}

func (v *ViewConfig) Apply(ctx context.Context, client *bigquery.Client, params map[string]string) error {
	dataset := client.Dataset(v.DatasetName)

	// check if the dataset exists.
	_, err := dataset.Metadata(ctx)
	if err != nil && hasStatusCode(err, http.StatusNotFound) {
		logrus.Infof("Dataset(%s) was not found. creating it...", dataset.DatasetID)
		err = dataset.Create(ctx, &bigquery.DatasetMetadata{
			Name: dataset.DatasetID,
		})
		if err != nil {
			logrus.Errorf("Failed to create dataset: %s", err.Error())
			return err
		}
	}

	view := client.Dataset(v.DatasetName).Table(v.ViewName)
	_, err = view.Metadata(ctx)
	if err == nil {
		logrus.Infof("View(%s.%s) existed. Deleting it...", view.DatasetID, view.TableID)
		view.Delete(ctx)
	}

	logrus.Infof("Creating view(%s.%s) ...", view.DatasetID, view.TableID)
	q, err := v.QueryWithParam(params)
	if err != nil {
		logrus.Errorf("Failed to execute template: %s", err.Error())
		return err
	}

	err = view.Create(ctx, &bigquery.TableMetadata{
		Name:           v.ViewName,
		ViewQuery:      q,
		UseStandardSQL: true,
	})
	if err != nil {
		logrus.Errorf("Faieled to create view: %s", err.Error())
		return err
	}
	return nil
}

func (v *ViewConfig) QueryWithParam(params map[string]string) (string, error) {
	t, err := template.New("q").Parse(v.Query)
	if err != nil {
		logrus.Errorf("Failed to parse query: %s", err.Error())
		return "", err
	}
	var buf bytes.Buffer
	if err = t.Execute(&buf, params); err != nil {
		logrus.Errorf("Failed to execute template: %s", err.Error())
		return "", err
	}
	return buf.String(), nil
}

func CreateViewConfigsFromDatasetDir(dir string) ([]*ViewConfig, error) {
	ret := make([]*ViewConfig, 0)
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		logrus.Panicf("Failed to list files in dir: %s", dir)
	}

	for _, f := range files {
		if !f.IsDir() {
			continue
		}
		err := createViewConfigsFromViewDir(filepath.Join(dir, f.Name()), &ret, f.Name())
		if err != nil {
			logrus.Panicf("Failed to readViews: %s", err.Error())
		}
	}

	return ret, nil
}

func createViewConfigsFromViewDir(dir string, ret *[]*ViewConfig, datasetName string) error {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		logrus.Panicf("Failed to list files in dir: %s", dir)
	}

	for _, f := range files {
		if !f.IsDir() {
			continue
		}
		viewConfig, err := createViewConfigFromQueryFile(datasetName, f.Name(), filepath.Join(dir, f.Name(), "query.sql"))
		if err != nil {
			return err
		}
		if viewConfig == nil {
			continue
		}

		*ret = append(*ret, viewConfig)
	}

	return nil
}

func createViewConfigFromQueryFile(datasetName, viewName, queryFileName string) (*ViewConfig, error) {
	if _, err := os.Stat(queryFileName); os.IsNotExist(err) {
		logrus.Debugf("File not found. skip %s.%s", datasetName, viewName)
		return nil, nil
	}

	queryFile, err := ioutil.ReadFile(queryFileName)
	if err != nil {
		logrus.Panicf("Failed to open query file(%s): %s", queryFileName, err.Error())
		return nil, err
	}

	query := string(queryFile[:])

	return &ViewConfig{
		DatasetName: datasetName,
		ViewName:    viewName,
		Query:       query,
	}, nil
}

// Copy and paste from go/bigquery/integration_test.go
func hasStatusCode(err error, code int) bool {
	if e, ok := err.(*googleapi.Error); ok && e.Code == code {
		return true
	}
	return false
}
