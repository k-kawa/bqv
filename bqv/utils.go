package bqv

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
)

// DeleteAllViews deletes all the views and returns true if it deletes any view.
func DeleteAllViews(ctx context.Context, client *bigquery.Client) (bool, error) {
	countDeletedTable := 0

	dit := client.Datasets(ctx)
	for {
		ds, err := dit.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			logrus.Errorf("Failed to iterate datasets: %s", err.Error())
			return false, err
		}

		tit := ds.Tables(ctx)
		for {
			t, err := tit.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				logrus.Errorf("Failed to iterate tables: %s", err.Error())
				continue
			}

			m, err := t.Metadata(ctx)
			if err != nil {
				logrus.Errorf("Failed to get metadata of table(%s): %s", t.TableID, err.Error())
				continue
			}
			if m.Type == bigquery.ViewTable {
				err := t.Delete(ctx)
				if err != nil {
					logrus.Errorf("Failed to delelete table(%s): %s", t.TableID, err.Error())
					continue
				}
			}
			logrus.Infof("Table(%s) was deleted", t.TableID)
			countDeletedTable++
		}
	}
	return countDeletedTable > 0, nil
}
