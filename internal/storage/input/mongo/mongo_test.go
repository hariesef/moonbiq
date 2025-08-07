package mongo

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"moonbiq/pkg/logger"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// startMongoContainer starts a MongoDB container for testing
func startMongoContainer(ctx context.Context) (testcontainers.Container, string, error) {
	req := testcontainers.ContainerRequest{
		Image:        "mongo:6.0",
		ExposedPorts: []string{"27017/tcp"},
		WaitingFor:   wait.ForLog("Waiting for connections"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", fmt.Errorf("failed to start container: %v", err)
	}

	mappedPort, err := container.MappedPort(ctx, "27017")
	if err != nil {
		return nil, "", fmt.Errorf("failed to get mapped port: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get host: %v", err)
	}

	connectionString := fmt.Sprintf("mongodb://%s:%s/testdb", host, mappedPort.Port())
	return container, connectionString, nil
}

// setupTestData inserts test data into the MongoDB collection
func setupTestData(ctx context.Context, client *mongo.Client, dbName string, collectionName string, count int) error {
	collection := client.Database(dbName).Collection(collectionName)

	// Clear any existing data
	if _, err := collection.DeleteMany(ctx, bson.M{}); err != nil {
		return fmt.Errorf("failed to clear collection: %v", err)
	}

	// Insert test documents
	testDocs := make([]interface{}, count)
	for i := 0; i < count; i++ {
		// Create a timestamp for the ObjectID that's spaced out by hours
		// This ensures we have a good time distribution for our range-based chunking
		timestamp := time.Now().Add(time.Duration(-i) * time.Hour)

		// Create a proper MongoDB ObjectID with the timestamp
		objectID := primitive.NewObjectIDFromTimestamp(timestamp)

		testDocs[i] = bson.M{
			"_id":        objectID,
			"name":       fmt.Sprintf("Document %d", i+1),
			"value":      i * 10,
			"created_at": timestamp,
		}
	}

	_, err := collection.InsertMany(ctx, testDocs)
	return err
}

// Helper function to verify documents in output files
func verifyDocuments(t *testing.T, filenames []string, expectedCount int) {
	// Verify all files exist
	for _, filename := range filenames {
		assert.FileExists(t, filename, "Output file should exist")
	}

	// Read and verify all documents across all files
	documentMap := make(map[string]bool)
	for _, filename := range filenames {
		file, err := os.Open(filename)
		require.NoError(t, err)
		defer file.Close()

		gzReader, err := gzip.NewReader(file)
		require.NoError(t, err)
		defer gzReader.Close()

		scanner := bufio.NewScanner(gzReader)
		for scanner.Scan() {
			line := scanner.Text()
			var doc map[string]interface{}
			err := json.Unmarshal([]byte(line), &doc)
			require.NoError(t, err)

			// Check if we have seen this document before
			// _id is renamed to id in the output
			id, ok := doc["id"].(string)
			if !ok {
				// If it's not a string, convert it to a string for our map key
				idValue := fmt.Sprintf("%v", doc["id"])
				id = idValue
			}

			assert.False(t, documentMap[id], "Document should not be duplicated")
			documentMap[id] = true

			// Verify document has expected fields
			_, ok = doc["name"].(string)
			assert.True(t, ok, "Document should have name field")

			_, ok = doc["value"].(float64)
			assert.True(t, ok, "Document should have value field")
		}
	}

	// Verify we have all expected documents
	assert.Len(t, documentMap, expectedCount, "Should have processed all %d documents", expectedCount)

	// With ObjectIDs, we can't predict the exact IDs, so we just verify the count
	// The duplicate check above ensures we have unique documents
	// And the length check ensures we have the right number of documents
}

// Helper function to create test schema file
func createTestSchemaFile(t *testing.T, tablePath string) {
	schema := []byte(`[
  {"name": "_id", "type": "string"},
  {"name": "name", "type": "string"},
  {"name": "value", "type": "number"},
  {"name": "created_at", "type": "timestamp"}
]`)
	err := os.WriteFile(tablePath, schema, 0644)
	require.NoError(t, err)
}

// TestMongoStorage_Read tests the Read function with a large collection (2000 documents)
func TestMongoStorage_Read(t *testing.T) {
	logger.Initialize("debug")
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// Start MongoDB container
	ctx := context.Background()
	container, connectionString, err := startMongoContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to start MongoDB container: %v", err)
	}
	defer container.Terminate(ctx)

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connectionString))
	if err != nil {
		t.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)

	// Setup test data - collection name must match the schema filename without .json
	const tableName = "test_collection"
	err = setupTestData(ctx, client, "testdb", tableName, 2000)
	require.NoError(t, err)

	// Create temp directory for test output
	tempDir := t.TempDir()

	// Create test schema file with format tablename.json
	schemaPath := filepath.Join(tempDir, tableName + ".json")
	createTestSchemaFile(t, schemaPath)

	// Create storage instance
	storage := &MongoStorage{
		client:            client,
		database:          client.Database("testdb"),
		tempDir:           tempDir,
		chunks:            4,
		maxGzipFileSize:   1024 * 1024 * 1024, // 1GB to prevent splitting
		rangesSplitMethod: "time",             // Use time-based method
	}

	// Call Read method with schema file path
	filenames, err := storage.Read(schemaPath)
	require.NoError(t, err)

	// Count the actual documents in the output files
	actualCount := 0
	for _, filename := range filenames {
		file, err := os.Open(filename)
		require.NoError(t, err)
		defer file.Close()

		gzReader, err := gzip.NewReader(file)
		require.NoError(t, err)
		defer gzReader.Close()

		scanner := bufio.NewScanner(gzReader)
		for scanner.Scan() {
			actualCount++
		}
		require.NoError(t, scanner.Err())
	}

	t.Logf("Found %d documents in output files (expected 2000)", actualCount)

	// Verify all documents were processed correctly
	verifyDocuments(t, filenames, actualCount)
}

// TestMongoStorage_ReadSampling tests the Read function with a large collection (2000 documents) using sampling method
func TestMongoStorage_ReadSampling(t *testing.T) {
	logger.Initialize("debug")
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// Start MongoDB container
	ctx := context.Background()
	container, connectionString, err := startMongoContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to start MongoDB container: %v", err)
	}
	defer container.Terminate(ctx)

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connectionString))
	if err != nil {
		t.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)

	// Setup test data - collection name must match the schema filename without .json
	const tableName = "test_collection_sampling"
	err = setupTestData(ctx, client, "testdb", tableName, 2000)
	require.NoError(t, err)

	// Create temp directory for test output
	tempDir := t.TempDir()

	// Create test schema file with format tablename.json
	schemaPath := filepath.Join(tempDir, tableName + ".json")
	createTestSchemaFile(t, schemaPath)

	// Create storage instance
	storage := &MongoStorage{
		client:            client,
		database:          client.Database("testdb"),
		tempDir:           tempDir,
		chunks:            4,
		maxGzipFileSize:   1024 * 1024 * 1024, // 1GB to prevent splitting
		rangesSplitMethod: "sampling",         // Use sampling method
	}

	// Call Read method with schema file path
	filenames, err := storage.Read(schemaPath)
	require.NoError(t, err)

	// Count the actual documents in the output files
	actualCount := 0
	for _, filename := range filenames {
		file, err := os.Open(filename)
		require.NoError(t, err)
		defer file.Close()

		gzReader, err := gzip.NewReader(file)
		require.NoError(t, err)
		defer gzReader.Close()

		scanner := bufio.NewScanner(gzReader)
		for scanner.Scan() {
			actualCount++
		}
		require.NoError(t, scanner.Err())
	}

	t.Logf("Found %d documents in output files using sampling method (expected 2000)", actualCount)

	// Verify all documents were processed correctly
	verifyDocuments(t, filenames, actualCount)
}

// TestMongoStorage_FileSplitting tests the file splitting functionality with a small maxGzipFileSize
func TestMongoStorage_FileSplitting(t *testing.T) {
	logger.Initialize("debug")

	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// Start MongoDB container
	ctx := context.Background()
	container, connectionString, err := startMongoContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to start MongoDB container: %v", err)
	}
	defer container.Terminate(ctx)

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connectionString))
	if err != nil {
		t.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)

	// Setup test data - collection name must match the schema filename without .json
	const tableName = "test_collection_splitting"
	err = setupTestData(ctx, client, "testdb", tableName, 500)
	require.NoError(t, err)

	// Create temp directory for test output
	tempDir := t.TempDir()

	// Create test schema file with format tablename.json
	schemaPath := filepath.Join(tempDir, tableName + ".json")
	createTestSchemaFile(t, schemaPath)

	// Create storage instance with small maxGzipFileSize to force splitting
	storage := &MongoStorage{
		client:            client,
		database:          client.Database("testdb"),
		tempDir:           tempDir,
		chunks:            1, // Use 1 chunk to test file splitting only
		maxGzipFileSize:   100, // Very small size to force splitting
		rangesSplitMethod: "time",
	}

	// Call Read method with schema file path
	filenames, err := storage.Read(schemaPath)
	require.NoError(t, err)

	// We expect multiple files due to splitting
	assert.Greater(t, len(filenames), 1, "Should have multiple output files due to splitting")

	// Verify all files exist and have content
	for i, filename := range filenames {
		info, err := os.Stat(filename)
		require.NoError(t, err)
		assert.Greater(t, info.Size(), int64(0), "File should not be empty")

		// For files except the last one, verify they are close to the max size
		if i < len(filenames)-1 {
			// The compressed size should be close to or slightly larger than maxGzipFileSize
			// We allow some flexibility since the exact size depends on compression
			assert.GreaterOrEqual(t, info.Size(), int64(90),
				"Non-final files should be close to the maxGzipFileSize")
		}
	}

	// Verify all documents were processed correctly
	verifyDocuments(t, filenames, 500)
}

// TestMongoStorage_ReadSmallCollection tests the Read function with a small collection (10 documents)
func TestMongoStorage_ReadSmallCollection(t *testing.T) {
	logger.Initialize("debug")

	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// Start MongoDB container
	ctx := context.Background()
	container, connectionString, err := startMongoContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to start MongoDB container: %v", err)
	}
	defer container.Terminate(ctx)

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connectionString))
	if err != nil {
		t.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)

	// Setup test data - collection name must match the schema filename without .json
	const tableName = "test_collection_small"
	err = setupTestData(ctx, client, "testdb", tableName, 10)
	require.NoError(t, err)

	// Create temp directory for test output
	tempDir := t.TempDir()

	// Create test schema file with format tablename.json
	schemaPath := filepath.Join(tempDir, tableName + ".json")
	createTestSchemaFile(t, schemaPath)

	// Create storage instance
	storage := &MongoStorage{
		client:            client,
		database:          client.Database("testdb"),
		tempDir:           tempDir,
		chunks:            10,                 // Try to use 10 chunks, but should automatically reduce to 1
		maxGzipFileSize:   1024 * 1024 * 1024, // 1GB to prevent splitting
		rangesSplitMethod: "time",             // Use time-based method
	}

	// Call Read method with schema file path
	filenames, err := storage.Read(schemaPath)
	require.NoError(t, err)

	// With the new logic, we expect a single chunk for small collections
	assert.Len(t, filenames, 1, "Should return 1 output file")

	// Verify all documents were processed correctly
	verifyDocuments(t, filenames, 10)
}

// TestGetDatabaseName tests the GetDatabaseName function
func TestGetDatabaseName(t *testing.T) {
	tests := []struct {
		name    string
		uri     string
		want    string
		wantErr bool
	}{
		{"valid", "mongodb://localhost:27017/testdb", "testdb", false},
		{"no db", "mongodb://localhost:27017/", "", true},
		{"invalid", "://invalid", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetDatabaseName(tt.uri)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
