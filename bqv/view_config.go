package bqv

import (
	"bytes"
	"context"
	"encoding/json"
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

// ViewConfig is...
type ViewConfig struct {
	Query       string
	ViewName    string
	DatasetName string
	Description string
}

// ViewDiff is...
type ViewDiff struct {
	ViewName       string
	DatasetName    string
	OldDescription string
	NewDescription string
	OldViewQuery   string
	NewViewQuery   string
}

// Apply creates the view or updates it when it existed.
// Apply returns (true, nil) if the view changed and (false ,nil) if the view didn't change
func (v *ViewConfig) Apply(ctx context.Context, client *bigquery.Client, params map[string]string) (bool, error) {
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
			return false, err
		}
	}

	view := client.Dataset(v.DatasetName).Table(v.ViewName)
	m, err := view.Metadata(ctx)
	if err == nil {
		if strings.Compare(m.ViewQuery, v.Query) == 0 {
			logrus.Infof("Skipping View(%s.%s). It exists and its query hasn't changed.", view.DatasetID, view.TableID)
			return false, nil
		}
		logrus.Infof("View(%s.%s) existed. Deleting it...", view.DatasetID, view.TableID)
		view.Delete(ctx)
	}

	logrus.Infof("Creating view(%s.%s) ...", view.DatasetID, view.TableID)
	q, err := v.QueryWithParam(params)
	if err != nil {
		logrus.Errorf("Failed to execute template: %s", err.Error())
		return false, err
	}

	err = view.Create(ctx, &bigquery.TableMetadata{
		Name:           v.ViewName,
		Description:    v.Description,
		ViewQuery:      q,
		UseStandardSQL: true,
	})
	if err != nil {
		logrus.Errorf("Failed to create view: %s", err.Error())
		return false, err
	}
	return true, nil
}

// DryRun tests Query is valid by executing the query in dry-run mode.
// DryRun returns true if the view might get created or updated when you call Apply and false if not.
func (v *ViewConfig) DryRun(ctx context.Context, client *bigquery.Client, params map[string]string) (bool, error) {
	m, err := v.getViewMetaDataIfExists(ctx, client)
	if err != nil {
		logrus.Errorf("Failed to get the metadata of this table: %s", err.Error())
		return false, err
	}
	q, err := v.QueryWithParam(params)
	if err != nil {
		logrus.Errorf("Failed to create query: %s", err.Error())
		return false, err
	}
	if strings.Compare(m.ViewQuery, q) == 0 {
		logrus.Infof("View(%s.%s) won't change", v.DatasetName, v.ViewName)
		return false, nil
	}

	query := client.Query(q)
	query.DryRun = true
	job, err := query.Run(ctx)
	if err != nil {
		logrus.Errorf("Failed to run the query: %s", err.Error())
		logrus.Errorf("query: %s", q)
		return false, err
	}

	// https://github.com/GoogleCloudPlatform/golang-samples/blob/master/bigquery/snippets/snippet.go#L1106
	// Dry run is not asynchronous, so get the latest status and statistics.
	jobStatus := job.LastStatus()
	if jobStatus.Err() != nil {
		logrus.Errorf("Dry run failed: %s", jobStatus.Err().Error())
		return true, jobStatus.Err()
	}

	logrus.Infof("View(%s.%s) seems OK", v.DatasetName, v.ViewName)
	return true, nil
}

func (v *ViewConfig) getViewMetaDataIfExists(ctx context.Context, client *bigquery.Client) (*bigquery.TableMetadata, error) {
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

// DeleteIfExist deletes the view if it exists.
// DeleteIfExist returns true if the view got deleted and false if not.
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

// QueryWithParam returns the SQL made of the template Query and the given params.
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

// Diff returns ViewDiff instance if the actual ViewQuery and the SQL made from Query and params are different.
func (v *ViewConfig) Diff(ctx context.Context, client *bigquery.Client, params map[string](string)) (*ViewDiff, error) {
	q, err := v.QueryWithParam(params)
	if err != nil {
		logrus.Errorf("Failed to get query: %s", err.Error())
		return nil, err
	}

	dataset := client.Dataset(v.DatasetName)
	if _, err = dataset.Metadata(ctx); err != nil && hasStatusCode(err, http.StatusNotFound) {
		return &ViewDiff{
			ViewName:       v.ViewName,
			DatasetName:    v.DatasetName,
			OldDescription: "",
			NewDescription: v.Description,
			OldViewQuery:   "",
			NewViewQuery:   q,
		}, nil
	}

	view := client.Dataset(v.DatasetName).Table(v.ViewName)
	m, err := view.Metadata(ctx)

	if err != nil && hasStatusCode(err, http.StatusNotFound) {
		return &ViewDiff{
			ViewName:       v.ViewName,
			DatasetName:    v.DatasetName,
			OldDescription: "",
			NewDescription: v.Description,
			OldViewQuery:   "",
			NewViewQuery:   q,
		}, nil
	}
	if (strings.Compare(m.ViewQuery, q) != 0) || (strings.Compare(m.Description, v.Description) != 0) {
		return &ViewDiff{
			ViewName:       v.ViewName,
			DatasetName:    v.DatasetName,
			OldDescription: m.Description,
			NewDescription: v.Description,
			OldViewQuery:   m.ViewQuery,
			NewViewQuery:   q,
		}, nil
	}
	return nil, nil
}

// CreateViewConfigsFromDatasetDir creates ViewConfig objects defined in the given dir directory.
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
		viewConfig, err := createViewConfigFromQueryFile(datasetName, f.Name(), filepath.Join(dir, f.Name(), "query.sql"), filepath.Join(dir, f.Name(), "meta.json"))
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

func createViewConfigFromQueryFile(datasetName, viewName, queryFileName, metadataFileName string) (*ViewConfig, error) {
	if _, err := os.Stat(queryFileName); os.IsNotExist(err) {
		logrus.Debugf("Query File not found. skip %s.%s", datasetName, viewName)
		return nil, nil
	}

	queryFile, err := ioutil.ReadFile(queryFileName)
	if err != nil {
		logrus.Errorf("Failed to open query file(%s): %s", queryFileName, err.Error())
		return nil, err
	}

	query := string(queryFile[:])

	var metadata map[string]interface{}
	if _, err := os.Stat(metadataFileName); os.IsNotExist(err) {
		logrus.Debugf("Metadata file not found. skip load metadata from file: %s", metadataFileName)
	} else {
		metadataFile, err := ioutil.ReadFile(metadataFileName)
		if err != nil {
			logrus.Errorf("Failed to open metadata file(%s): %s", metadataFileName, err.Error())
			return nil, err
		}
		if err := json.Unmarshal(metadataFile, &metadata); err != nil {
			logrus.Errorf("Failed to extract metadata json data(%s): %s", metadataFileName, err.Error())
			return nil, err
		}
		logrus.Debugf("metadata:%s", metadata)
	}

	description := string("")

	if metadata["description"] != nil {
		description = metadata["description"].(string)
	}

	return &ViewConfig{
		DatasetName: datasetName,
		ViewName:    viewName,
		Description: description,
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
