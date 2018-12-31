package bqv

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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

type ViewDiff struct {
	ViewName     string
	DatasetName  string
	OldViewQuery string
	NewViewQuery string
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
	m, err := view.Metadata(ctx)
	if err == nil {
		if strings.Compare(m.ViewQuery, v.Query) == 0 {
			logrus.Infof("Skipping View(%s.%s). It exists and its query hasn't changed.", view.DatasetID, view.TableID)
			return nil
		}
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

func (v *ViewConfig) DryRun(ctx context.Context, client *bigquery.Client, params map[string]string) error {
	m, err := v.GetViewMetaDataIfExists(ctx, client)
	if err != nil {
		logrus.Errorf("Failed to get the metadata of this table: %s", err.Error())
		return err
	}
	q, err := v.QueryWithParam(params)
	if err != nil {
		logrus.Errorf("Failed to create query: %s", err.Error())
		return err
	}
	if strings.Compare(m.ViewQuery, q) == 0 {
		logrus.Infof("View(%s.%s) won't change", v.DatasetName, v.ViewName)
		return nil
	}

	query := client.Query(q)
	query.DryRun = true
	job, err := query.Run(ctx)
	if err != nil {
		logrus.Errorf("Failed to run the query: %s", err.Error())
		logrus.Errorf("query: %s", q)
		return err
	}

	// https://github.com/GoogleCloudPlatform/golang-samples/blob/master/bigquery/snippets/snippet.go#L1106
	// Dry run is not asynchronous, so get the latest status and statistics.
	jobStatus := job.LastStatus()
	if jobStatus.Err() != nil {
		logrus.Errorf("Dry run failed: %s", jobStatus.Err().Error())
		return jobStatus.Err()
	}

	logrus.Infof("View(%s.%s) seems OK", v.DatasetName, v.ViewName)
	return nil
}

func (v *ViewConfig) GetViewMetaDataIfExists(ctx context.Context, client *bigquery.Client) (*bigquery.TableMetadata, error) {
	dataset := client.Dataset(v.DatasetName)
	_, err := dataset.Metadata(ctx)
	if err != nil && hasStatusCode(err, http.StatusNotFound) {
		logrus.Debugf("Dataset(%s) didn't exist.", v.DatasetName)
		return nil, nil
	}
	view := client.Dataset(v.DatasetName).Table(v.ViewName)
	m, err := view.Metadata(ctx)
	if err == nil {
		logrus.Debugf("View(%s.%s) was found", v.DatasetName, v.ViewName)
		return m, nil
	}
	return nil, nil
}

func (v *ViewConfig) DeleteIfExist(ctx context.Context, client *bigquery.Client) (bool, error) {
	dataset := client.Dataset(v.DatasetName)
	_, err := dataset.Metadata(ctx)
	if err != nil && hasStatusCode(err, http.StatusNotFound) {
		logrus.Debugf("Dataset(%s) didn't exist.", v.DatasetName)
		return false, nil
	}
	view := client.Dataset(v.DatasetName).Table(v.ViewName)

	_, err = view.Metadata(ctx)
	if err == nil {
		logrus.Debugf("View(%s.%s) was found. deleteing...", v.DatasetName, v.ViewName)
		view.Delete(ctx)
		return true, nil
	}
	return false, nil
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

func (v *ViewConfig) Diff(ctx context.Context, client *bigquery.Client, params map[string](string)) (*ViewDiff, error) {
	q, err := v.QueryWithParam(params)
	if err != nil {
		logrus.Errorf("Failed to get query: %s", err.Error())
		return nil, err
	}

	dataset := client.Dataset(v.DatasetName)
	if _, err = dataset.Metadata(ctx); err != nil && hasStatusCode(err, http.StatusNotFound) {
		return &ViewDiff{
			ViewName:     v.ViewName,
			DatasetName:  v.DatasetName,
			OldViewQuery: "",
			NewViewQuery: q,
		}, nil
	}

	view := client.Dataset(v.DatasetName).Table(v.ViewName)

	m, err := view.Metadata(ctx)
	if err != nil && hasStatusCode(err, http.StatusNotFound) {
		return &ViewDiff{
			ViewName:     v.ViewName,
			DatasetName:  v.DatasetName,
			OldViewQuery: "",
			NewViewQuery: q,
		}, nil
	}
	if strings.Compare(m.ViewQuery, q) != 0 {
		return &ViewDiff{
			ViewName:     v.ViewName,
			DatasetName:  v.DatasetName,
			OldViewQuery: m.ViewQuery,
			NewViewQuery: q,
		}, nil
	}
	return nil, nil
}

func CreateViewConfigsFromDatasetDir(dir string) ([]*ViewConfig, error) {
	ret := make([]*ViewConfig, 0)
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		logrus.Errorf("Failed to list files in dir: %s", dir)
		return nil, err
	}

	for _, f := range files {
		if !f.IsDir() {
			continue
		}
		err := createViewConfigsFromViewDir(filepath.Join(dir, f.Name()), &ret, f.Name())
		if err != nil {
			logrus.Errorf("Failed to create views in the dir(%s): %s", filepath.Join(dir, f.Name()), err.Error())
			// Keep creating ViewConfigs in the next dir instead of returning err.
		}
	}

	return ret, nil
}

func createViewConfigsFromViewDir(dir string, ret *[]*ViewConfig, datasetName string) error {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		logrus.Errorf("Failed to list files in dir: %s", dir)
		return err
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
		logrus.Errorf("Failed to open query file(%s): %s", queryFileName, err.Error())
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
