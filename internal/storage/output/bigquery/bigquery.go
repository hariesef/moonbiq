package bigquery

import (
	"context"
	"fmt"
	"moonbiq/internal/network/gcpimpl"
	"moonbiq/pkg/helper"
	"moonbiq/pkg/logger"
	"moonbiq/pkg/network/gcplayer"
	"path/filepath"
	"time"
)

type BigQueryStorage struct {
	projectID      string
	tempDataSet    string
	finalDataset   string
	useTempDataset bool
	gcsBucket      string
	tempDir        string
	config         map[string]interface{}

	// Clients
	gcpLayer gcplayer.GcpLayer
	ctx      context.Context
}

func New() *BigQueryStorage {
	return &BigQueryStorage{
		ctx: context.Background(),
	}
}

func (bq *BigQueryStorage) Setup(config map[string]interface{}) {
	var ok bool
	bq.projectID, ok = config["projectId"].(string)
	if !ok || bq.projectID == "" {
		panic("projectId is required in BigQuery config")
	}

	bq.tempDataSet, _ = config["tempDataSet"].(string)
	bq.finalDataset, _ = config["finalDataset"].(string)
	bq.useTempDataset, _ = config["useTempDataset"].(bool)
	bq.gcsBucket, _ = config["gcsBucket"].(string)
	bq.tempDir, _ = config["tempDir"].(string)
	bq.config = config

	// Initialize GCP Network layer
	bq.gcpLayer = gcpimpl.New()
	if err := bq.gcpLayer.Setup(bq.ctx, bq.gcsBucket, bq.projectID, bq.GetDataset()); err != nil {
		panic("Failed to setup GCP layer: " + err.Error())
	}

}

func (bq *BigQueryStorage) Close() error {
	return bq.gcpLayer.Close()
}

func (bq *BigQueryStorage) CheckTableExists(tablePath string) (bool, string, error) {
	dataset := bq.GetDataset()
	logger.Info("checking table %s in dataset %s", tablePath, dataset)
	return bq.gcpLayer.CheckTableExists(tablePath, dataset)
}

func (bq *BigQueryStorage) gcsUploadWorker(id int, jobs <-chan string,
	results chan<- error, folderPath string) {

	for file := range jobs {
		// Use folder path + filename for GCS object name
		objectName := filepath.Join(folderPath, filepath.Base(file))

		logger.Info("worker %d uploading %s to GCS", id, file)
		if err := bq.gcpLayer.UploadToGcs(objectName, file); err != nil {
			results <- fmt.Errorf("worker %d failed to upload %s: %w", id, file, err)
			continue
		}

		logger.Info("worker %d successfully uploaded %s to GCS", id, file)
		results <- nil
	}
}

func (bq *BigQueryStorage) GetDataset() string {
	if bq.useTempDataset {
		return bq.tempDataSet
	}
	return bq.finalDataset
}

func (bq *BigQueryStorage) Write(tablePath string, files []string, overwriteTable bool) error {
	dataset := bq.GetDataset()
	exists, tableName, err := bq.gcpLayer.CheckTableExists(tablePath, dataset)
	if err != nil {
		return err
	}

	if !exists {
		logger.Info("table %s on dataset %s does not exist, creating it", tableName, dataset)
		if err := bq.gcpLayer.CreateTable(tablePath, dataset, tableName); err != nil {
			return err
		}
	}

	if exists && !overwriteTable {
		logger.Info("table %s on dataset %s already exists, skipping (use -overwrite to force)", tableName, dataset)
		return nil
	} else if exists && overwriteTable {
		logger.Info("table %s on dataset %s already exists, overwriting it", tableName, dataset)
		// Drop the table
		if err := bq.gcpLayer.DeleteTable(dataset, tableName); err != nil && !helper.IsNotFoundError(err) {
			return err
		}
		if err := bq.gcpLayer.CreateTable(tablePath, dataset, tableName); err != nil {
			return err
		}
	}

	// Setup parallel upload
	numWorkers := 1
	if val, ok := bq.config["gcsParallelUploads"].(float64); ok {
		numWorkers = int(val)
	}
	logger.Info("using %d workers for GCS upload", numWorkers)

	jobs := make(chan string, len(files))
	results := make(chan error, len(files))

	// Create timestamped folder name
	timestamp := time.Now().Format("20060102-150405")
	folderPath := fmt.Sprintf("%s-%s", tableName, timestamp)

	// Start workers
	for w := 1; w <= numWorkers; w++ {
		go bq.gcsUploadWorker(w, jobs, results, folderPath)
	}

	// Send jobs
	for _, file := range files {
		jobs <- file
	}
	close(jobs)

	// Collect results
	for range files {
		if err := <-results; err != nil {
			return fmt.Errorf("GCS upload failed: %w", err)
		}
	}

	gcsPath := fmt.Sprintf("gs://%s/%s/*", bq.gcsBucket, folderPath)
	logger.Info("Loading from GCS to Big Query: %s", gcsPath)
	totalUploaded, err := bq.gcpLayer.LoadToBigQuery(gcsPath, dataset, tableName)
	if err != nil {
		return err
	}
	logger.Info("Successfully uploaded %d rows to Big Query for table %s", totalUploaded, tableName)

	// Clean up GCS files we just uploaded
	for _, file := range files {
		objectName := fmt.Sprintf("%s/%s", folderPath, filepath.Base(file))
		logger.Debug("cleaning up GCS object %s", objectName)
		if err := bq.gcpLayer.DeleteGcsFile(objectName); err != nil {
			logger.Error("failed to delete GCS object %s: %v", objectName, err)
		}
	}
	logger.Info("cleaned up %d temporary files from GCS", len(files))

	if bq.useTempDataset {
		//this one has mode overwrite/truncate destination
		logger.Info("copying temp table %s in dataset %s to final table %s in dataset %s", tableName, bq.tempDataSet, tableName, bq.finalDataset)
		if err := bq.gcpLayer.CopyTable(bq.tempDataSet, tableName, bq.finalDataset, tableName); err != nil {
			return fmt.Errorf("failed to copy table: %w", err)
		}
	}

	logger.Info("successfully loaded data into BigQuery table %s", tableName)
	return nil
}
