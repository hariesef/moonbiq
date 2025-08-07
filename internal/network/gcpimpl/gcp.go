package gcpimpl

import (
	"context"
	"fmt"
	"io"
	"moonbiq/pkg/helper"
	"moonbiq/pkg/logger"
	"os"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
)

type GcpImpl struct {
	gcsBucket string
	projectId string
	dataset   string

	// Clients
	gcsClient *storage.Client
	bqClient  *bigquery.Client
	ctx       context.Context
}

func New() *GcpImpl {
	return &GcpImpl{}
}

func (g *GcpImpl) Setup(ctx context.Context, bucketId string, projectId string,
	dataset string) error {
	g.ctx = ctx
	g.gcsBucket = bucketId
	g.projectId = projectId
	g.dataset = dataset

	// Initialize GCS client
	gcsClient, err := storage.NewClient(g.ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCS client: %w", err)
	}
	g.gcsClient = gcsClient
	// Test GCS connection
	_, err = g.gcsClient.Bucket(g.gcsBucket).Attrs(g.ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to GCS bucket: %w", err)
	}

	// Initialize BigQuery client
	bqClient, err := bigquery.NewClient(g.ctx, g.projectId)
	if err != nil {
		return fmt.Errorf("failed to create BigQuery client: %w", err)
	}
	g.bqClient = bqClient
	// Test BigQuery connection
	_, err = g.bqClient.Dataset(g.dataset).Metadata(g.ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to BigQuery dataset: %w", err)
	}

	return nil
}

func (g *GcpImpl) Close() error {
	var errs []error

	if g.gcsClient != nil {
		if err := g.gcsClient.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close GCS client: %w", err))
		}
	}

	if g.bqClient != nil {
		if err := g.bqClient.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close BigQuery client: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing clients: %v", errs)
	}
	return nil
}

func (g *GcpImpl) CheckTableExists(tablePath string, dataset string) (bool, string, error) {
	tableName := helper.NormalizeTableName(tablePath)
	if tableName == "" {
		return false, "", fmt.Errorf("invalid table name for path: %s", tablePath)
	}

	table := g.bqClient.Dataset(dataset).Table(tableName)
	_, err := table.Metadata(g.ctx)
	if err != nil {
		if helper.IsNotFoundError(err) {
			return false, tableName, nil
		}
		return false, tableName, fmt.Errorf("failed to check table metadata: %w", err)
	}
	return true, tableName, nil
}

func (g *GcpImpl) DeleteTable(dataset string, tableName string) error {
	if err := g.bqClient.Dataset(dataset).Table(tableName).Delete(g.ctx); err != nil && !helper.IsNotFoundError(err) {
		return fmt.Errorf("failed to delete final table: %w", err)
	}

	return nil
}

func (g *GcpImpl) CreateTable(tablePath string, dataset string, tableName string) error {
	meta, err := g.createTableMetadata(tablePath)
	if err != nil {
		return fmt.Errorf("failed to get table metadata: %w", err)
	}
	if err := g.bqClient.Dataset(dataset).Table(tableName).Create(g.ctx, meta); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

func (g *GcpImpl) createTableMetadata(tablePath string) (*bigquery.TableMetadata, error) {
	// Read schema from file
	schemaBytes, err := os.ReadFile(tablePath)
	if err != nil {
		return nil, fmt.Errorf("schema file %s not found: %w", tablePath, err)
	}

	// Parse schema from file
	schema, err := bigquery.SchemaFromJSON(schemaBytes)
	if err != nil {
		return nil, fmt.Errorf("invalid schema in %s: %w", tablePath, err)
	}

	if len(schema) == 0 {
		return nil, fmt.Errorf("empty schema in %s", tablePath)
	}

	logger.Info("using schema from %s", tablePath)
	return &bigquery.TableMetadata{Schema: schema}, nil
}

func (g *GcpImpl) UploadToGcs(objectName string, file string) error {

	// Open the file
	f, err := os.Open(file)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", file, err)
	}

	// Upload to GCS
	obj := g.gcsClient.Bucket(g.gcsBucket).Object(objectName)
	w := obj.NewWriter(g.ctx)

	if _, err := io.Copy(w, f); err != nil {
		f.Close()
		return fmt.Errorf("failed to upload %s: %w", file, err)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to finalize upload %s: %w", file, err)
	}
	f.Close()

	return nil
}

func (g *GcpImpl) DeleteGcsFile(objectName string) error {
	if err := g.gcsClient.Bucket(g.gcsBucket).Object(objectName).Delete(g.ctx); err != nil {
		return fmt.Errorf("failed to delete GCS object %s: %v", objectName, err)
	}
	return nil
}

func (g *GcpImpl) LoadToBigQuery(gcsPath string, dataset string, tableName string) (int64, error) {

	gcsRef := bigquery.NewGCSReference(gcsPath)
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.IgnoreUnknownValues = true // This is to ignore unknown fields in the JSON
	gcsRef.AllowQuotedNewlines = true
	gcsRef.AllowJaggedRows = true

	loader := g.bqClient.Dataset(dataset).Table(tableName).LoaderFrom(gcsRef)
	// WriteDisposition: Controls write behavior to existing tables
	// WRITE_TRUNCATE: Overwrites existing table data
	loader.WriteDisposition = bigquery.WriteTruncate
	// CreateDisposition: Controls table creation if it doesn't exist
	// CREATE_NEVER: Fails if table doesn't exist (we create it explicitly earlier)
	loader.CreateDisposition = bigquery.CreateNever

	job, err := loader.Run(g.ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to start load job: %w", err)
	}

	status, err := job.Wait(g.ctx)
	if err != nil {
		return 0, fmt.Errorf("load job failed: %w", err)
	}
	if err := status.Err(); err != nil {
		return 0, fmt.Errorf("load job completed with error: %w", err)
	}

	// Get the number of output rows
	stats, ok := status.Statistics.Details.(*bigquery.LoadStatistics)
	if !ok {
		return 0, fmt.Errorf("failed to get load statistics")
	}
	return stats.OutputRows, nil
}

func (g *GcpImpl) CopyTable(srcDataset, srcTable, dstDataset, dstTable string) error {
	src := g.bqClient.Dataset(srcDataset).Table(srcTable)
	dst := g.bqClient.Dataset(dstDataset).Table(dstTable)
	copier := dst.CopierFrom(src)
	copier.WriteDisposition = bigquery.WriteTruncate
	job, err := copier.Run(g.ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(g.ctx)
	if err != nil {
		return err
	}
	return status.Err()
}
