package bqv

import (
	"context"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
)

func TestApply(t *testing.T) {
	ctx := context.Background()
	v := &ViewConfig{
		DatasetName: "test",
		ViewName:    "test",
		Query:       "SELECT 1 AS one",
	}

	projectID, ok := os.LookupEnv("projectID")
	if !ok {
		t.Error("projectID is required.")
	}

	if strings.Compare(projectID, "") == 0 {
		t.Error("projectID's not been given")
	}

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		t.Error("Failed to create a bigquery client.")
		return
	}

	_, err = v.DeleteIfExist(ctx, client)
	if err != nil {
		t.Errorf("Failed to cleanup view(%s.%s)", v.DatasetName, v.ViewName)
	}

	created, err := v.Apply(ctx, client, nil)
	if err != nil {
		t.Error("Failed to apply the viewconfig.")
	}
	if !created {
		t.Errorf("View(%s.%s) should have been created. but haven't", v.DatasetName, v.ViewName)
	}

	created, err = v.Apply(ctx, client, nil)
	if err != nil {
		t.Error("Failed to apply the viewconfig.")
	}
	if created {
		t.Error("View creation should have been skipped.")
	}
}
