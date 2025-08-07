package mongo

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"moonbiq/pkg/logger"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const chunkDefault = 10

// chunkResult holds the result of processing a chunk
type chunkResult struct {
	filenames []string
	err       error
}

// IdRange represents a range of _id values for chunking
type IdRange struct {
	startID primitive.ObjectID
	endID   primitive.ObjectID
}

// GetStartID returns the start ID of the range
func (r IdRange) GetStartID() primitive.ObjectID {
	return r.startID
}

// GetEndID returns the end ID of the range
func (r IdRange) GetEndID() primitive.ObjectID {
	return r.endID
}

// MongoStorage implements the generic.InputStorage interface for MongoDB
type MongoStorage struct {
	client            *mongo.Client
	uri               string
	database          *mongo.Database
	tempDir           string
	chunks            int   // Number of chunks to use for parallel processing
	maxGzipFileSize   int64 // Maximum size of gzipped output files in bytes
	rangesSplitMethod string
	fields            map[string]string
	processedDocCount int64
}

// New creates a new MongoDB storage instance
func New() *MongoStorage {
	return &MongoStorage{
		tempDir:           "temp",
		maxGzipFileSize:   1 * 1024 * 1024 * 1024, // Default 1GB
		processedDocCount: 0,
	}
}

// GetDatabaseName extracts the database name from MongoDB URI
func GetDatabaseName(uri string) (string, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		logger.Error("failed to parse mongodb uri: %v", err)
		return "", fmt.Errorf("failed to parse mongodb uri: %v", err)
	}

	// The path should be in the format "/databaseName"
	path := parsed.Path
	if len(path) < 2 {
		errMsg := "no database name found in uri"
		logger.Error(errMsg)
		return "", errors.New(errMsg)
	}

	// Remove the leading "/" and return the database name
	return path[1:], nil
}

// New creates a new MongoDB storage instance with the given configuration
func (m *MongoStorage) Setup(config map[string]interface{}) error {
	// Get MongoDB URI from config or environment variable
	mongoURI, ok := config["uri"].(string)
	if !ok || mongoURI == "" {
		mongoURI = os.Getenv("MONGO_URI")
		if mongoURI == "" {
			logger.Error("mongodb uri not provided in config or environment")
			return errors.New("mongodb uri not provided in config or environment")
		}
	}

	logger.Info("Connecting to mongodb server...")
	// Create MongoDB client
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		logger.Error("failed to create mongodb client: %v", err)
		return fmt.Errorf("failed to create mongodb client: %v", err)
	}

	// Get database name from URI
	dbName, err := GetDatabaseName(mongoURI)
	if err != nil {
		logger.Error("failed to get database name: %v", err)
		return err
	}

	// Test the connection
	err = client.Ping(context.Background(), nil)
	if err != nil {
		logger.Error("failed to ping mongodb: %v", err)
		return fmt.Errorf("failed to ping mongodb: %v", err)
	}

	// Get temp directory from config or use default

	tempDirConfig := os.Getenv("MOONBIQ_TEMP")
	if tempDirConfig == "" {
		tempDirConfig = m.tempDir // Use the default tempDir from the MongoStorage instance
	}

	// Get chunks configuration from config or use default
	chunks := chunkDefault                                  // Default to 10 chunks
	if chunksConfig, ok := config["chunks"].(float64); ok { // JSON numbers are parsed as float64
		chunks = int(chunksConfig)
		logger.Debug("using configured chunk count: %d", chunks)
	}

	// Get maxGzipFileSize configuration from config or use default (1GB)
	maxGzipFileSize := int64(1 * 1024 * 1024 * 1024) // Default to 1GB
	if maxSizeConfig, ok := config["maxGzipFileSize"].(float64); ok {
		maxGzipFileSize = int64(maxSizeConfig)
		logger.Debug("using configured max gzip file size: %d bytes", maxGzipFileSize)
	}

	// Get rangesSplitMethod configuration from config or use default (time)
	rangesSplitMethod := "time" // Default to time-based method
	if methodConfig, ok := config["rangesSplitMethod"].(string); ok {
		rangesSplitMethod = methodConfig
		logger.Debug("using configured ranges split method: %s", rangesSplitMethod)
	}

	// Create temp directory if it doesn't exist
	if err := os.MkdirAll(tempDirConfig, 0755); err != nil {
		logger.Error("failed to create temp directory: %v", err)
		return fmt.Errorf("failed to create temp directory: %v", err)
	}

	logger.Info("successfully connected to mongodb")
	m.client = client
	m.uri = mongoURI
	m.database = client.Database(dbName)
	m.tempDir = tempDirConfig
	m.chunks = chunks
	m.maxGzipFileSize = maxGzipFileSize
	m.rangesSplitMethod = rangesSplitMethod
	return nil
}

// Close closes the MongoDB connection
func (m *MongoStorage) Close() error {
	if m.client == nil {
		return nil
	}
	return m.client.Disconnect(context.Background())
}

func (m *MongoStorage) GetCollection(tableName string) *mongo.Collection {
	return m.database.Collection(tableName)
}

// Read reads data from MongoDB and returns the list of generated files
func (m *MongoStorage) Read(tablePath string) ([]string, error) {
	logger.Info("starting to read collection: %s", tablePath)
	base := filepath.Base(tablePath)
	tableName := strings.TrimSuffix(base, filepath.Ext(base))
	logger.Info("mongo table name: %s", tableName)

	if tableName == "" {
		return nil, fmt.Errorf("invalid table name for path: %s", tablePath)
	}

	fields, err := loadSchema(tablePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load schema from json file: %v", err)
	}
	m.fields = fields

	// Get the collection
	collection := m.database.Collection(tableName)

	// Determine number of chunks (use configured value, but enforce restrictions)
	chunks := m.chunks
	if chunks <= 0 {
		logger.Info("using default chunk count: 10 (was not set)")
		chunks = chunkDefault // Default if not set
	}
	if chunks > 10 {
		logger.Info("limiting to maximum chunk count: 10 (was %d)", chunks)
		chunks = chunkDefault // Maximum allowed
	}

	totalDocs := int64(-1) //default value
	if m.chunks > 1 && m.rangesSplitMethod == "time" {
		// Count total documents only when chunks is > 1
	}

	logger.Debug("using %d chunks for processing collection %s", chunks, tableName)

	var ranges []IdRange

	rangeFileFolderLocation := os.Getenv("MOONBIQ_RANGE_FILE_FOLDER")
	if rangeFileFolderLocation == "" {
		rangeFileFolderLocation = "config/ranges"
	}
	rangeFileLocation := rangeFileFolderLocation + "/" + tableName + ".json"
	//try to read ranges from file (optional)
	logger.Info("trying to read ranges from file (if exists): %s", rangeFileLocation)
	rangesFromFile, err := m.ReadIDRangesFromFile(rangeFileLocation)
	if err == nil {
		ranges = rangesFromFile
		m.rangesSplitMethod = "file"
		chunks = len(rangesFromFile)
	} else {
		logger.Info("failed to read ranges from file: %v, continuing with other ranges method.", err)
	}

	// Choose ID ranges method based on configuration
	if chunks == 1 {
		// Use single chunk for small collections
		logger.Info("using single chunk for small collection: %d documents", totalDocs)
		ranges = []IdRange{{startID: primitive.ObjectID{}, endID: primitive.ObjectID{}}}
	} else {
		logger.Info("getting id ranges split for collection %s using method: %s", tableName, m.rangesSplitMethod)
		switch m.rangesSplitMethod {
		case "time":
			// Use time-based approximation method
			logger.Debug("using time-based objectid ranges method")
			ranges, err = m.GetIDRanges2(collection, chunks)
		case "sampling":
			// Use sampling-based method
			logger.Debug("using sampling-based ranges method")
			ranges, err = m.GetIDRanges(collection, chunks)
		case "file":
			// Use ranges from file
			logger.Debug("using ranges from file: %s", rangeFileLocation)
		case "count":
			// Use counting-based method
			logger.Debug("using counting-based ranges method")
			ranges, err = m.GetIDRanges3(collection, chunks)
		default:
			// Default to sampling method
			logger.Warn("unknown ranges split method '%s', defaulting to sampling method", m.rangesSplitMethod)
			ranges, err = m.GetIDRanges(collection, chunks)
		}

		if err != nil {
			logger.Error("failed to get id ranges: %v", err)
			return nil, fmt.Errorf("failed to get id ranges: %v", err)
		}
	}

	logger.Debug("created %d id ranges for collection %s", len(ranges), tableName)

	// Process chunks in parallel
	chunkCount := len(ranges)
	logger.Info("processing %d chunks in parallel", chunkCount)
	var wg sync.WaitGroup
	resultChan := make(chan chunkResult, chunkCount)
	for i, r := range ranges {
		wg.Add(1)
		go func(chunkNum int, r IdRange) {
			defer wg.Done()
			logger.Debug("starting chunk %d", chunkNum)
			filenames, err := m.processChunk(collection, tableName, chunkNum, r)
			if err != nil {
				logger.Error("error in chunk %d: %v", chunkNum, err)
			} else {
				logger.Debug("completed chunk %d, output to %v", chunkNum, filenames)
			}
			resultChan <- chunkResult{filenames: filenames, err: err}
		}(i, r)
	}

	// Close the result channel when all goroutines are done
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	var result []string
	var errors []error

	for res := range resultChan {
		if res.err != nil {
			errors = append(errors, res.err)
		} else if len(res.filenames) > 0 {
			result = append(result, res.filenames...)
		}
	}

	// If there were any errors, return them
	if len(errors) > 0 {
		errMsg := fmt.Sprintf("error processing %d chunks: %v", len(errors), errors)
		logger.Error(errMsg)
		return result, fmt.Errorf("error processing chunks: %v", errors)
	}

	logger.Info("successfully processed %d chunks for collection %s", len(ranges), tableName)
	return result, nil
}

// GetIDRanges2 calculates ID ranges for chunking based on ObjectID timestamp approximation
// It uses the fact that MongoDB ObjectIDs contain a timestamp component
// By finding the oldest and newest ObjectIDs, we can create evenly distributed chunks
func (m *MongoStorage) GetIDRanges2(collection *mongo.Collection, chunks int) ([]IdRange, error) {
	// If we're configured to use only 1 chunk, return a single range from first to last document
	if chunks == 1 {
		logger.Debug("using single chunk as configured - processing all documents in one range")
		// For a single chunk, we want to include all documents from beginning to end
		return []IdRange{{startID: primitive.NilObjectID, endID: primitive.NilObjectID}}, nil
	}

	logger.Info("counting documents in collection: %s", collection.Name())
	totalDocs, err := collection.CountDocuments(context.Background(), bson.M{})
	if err != nil {
		logger.Error("failed to count documents: %v", err)
		return nil, fmt.Errorf("failed to count documents: %v", err)
	}
	logger.Info("found %d documents in collection %s", totalDocs, collection.Name())
	// Reduce chunks if fewer documents
	if totalDocs < 1000 {
		logger.Info("using single chunk because document count is small: %d documents", totalDocs)
		chunks = 1 // Force single chunk for small collections
	}

	// Find the oldest document (with smallest _id)
	oldestDoc := bson.M{}
	err = collection.FindOne(
		context.Background(),
		bson.M{},
		options.FindOne().SetSort(bson.M{"_id": 1}),
	).Decode(&oldestDoc)

	if err != nil {
		logger.Error("failed to find oldest document: %v", err)
		return nil, fmt.Errorf("failed to find oldest document: %v", err)
	}

	// Find the newest document (with largest _id)
	newestDoc := bson.M{}
	err = collection.FindOne(
		context.Background(),
		bson.M{},
		options.FindOne().SetSort(bson.M{"_id": -1}),
	).Decode(&newestDoc)

	if err != nil {
		logger.Error("failed to find newest document: %v", err)
		return nil, fmt.Errorf("failed to find newest document: %v", err)
	}

	// Extract ObjectIDs
	oldestID, ok := oldestDoc["_id"].(primitive.ObjectID)
	if !ok {
		logger.Error("oldest document _id is not an objectid")
		return nil, fmt.Errorf("oldest document _id is not an objectid")
	}

	newestID, ok := newestDoc["_id"].(primitive.ObjectID)
	if !ok {
		logger.Error("newest document _id is not an objectid")
		return nil, fmt.Errorf("newest document _id is not an objectid")
	}

	// Get timestamps from ObjectIDs
	oldestTime := oldestID.Timestamp()
	newestTime := newestID.Timestamp()

	logger.Debug("oldest document timestamp: %v", oldestTime)
	logger.Debug("newest document timestamp: %v", newestTime)

	// Calculate time range
	timeRange := newestTime.Sub(oldestTime)
	if timeRange <= 0 {
		logger.Warn("time range is zero or negative, using single chunk")
		return []IdRange{{startID: primitive.NilObjectID, endID: primitive.NilObjectID}}, nil
	}

	// Calculate documents per chunk (approximately)
	docsPerChunk := totalDocs / int64(chunks)
	logger.Debug("targeting approximately %d documents per chunk", docsPerChunk)

	// Create chunks based on time boundaries
	ranges := make([]IdRange, chunks)
	for i := 0; i < chunks; i++ {
		// Calculate boundary percentages
		startPercent := float64(i) / float64(chunks)
		endPercent := float64(i+1) / float64(chunks)

		// Calculate boundary times
		startDuration := time.Duration(int64(float64(timeRange) * startPercent))
		endDuration := time.Duration(int64(float64(timeRange) * endPercent))

		startTime := oldestTime.Add(startDuration)
		endTime := oldestTime.Add(endDuration)

		// Create ObjectIDs at these times
		var startID, endID primitive.ObjectID

		// For the first chunk, use nil as startID to include all documents from the beginning
		if i == 0 {
			startID = primitive.NilObjectID
		} else {
			// Create an ObjectID with the calculated timestamp
			// Add a small random component to the ObjectID to ensure better distribution
			startID = primitive.NewObjectIDFromTimestamp(startTime)
		}

		// For the last chunk, use nil as endID to include all documents until the end
		if i == chunks-1 {
			endID = primitive.NilObjectID
		} else {
			// Create an ObjectID with the calculated timestamp
			// Add a small random component to the ObjectID to ensure better distribution
			endID = primitive.NewObjectIDFromTimestamp(endTime)
		}

		// Store the range
		ranges[i] = IdRange{
			startID: startID,
			endID:   endID,
		}

		logger.Debug("chunk %d range: %v to %v (objectids: %v to %v)", i, startTime, endTime, startID.Hex(), endID.Hex())
	}

	return ranges, nil
}

// GetIDRanges calculates ID ranges for chunking based on sampling
func (m *MongoStorage) GetIDRanges(collection *mongo.Collection, chunks int) ([]IdRange, error) {
	// If we're configured to use only 1 chunk, return a single range from first to last document
	if chunks == 1 {
		logger.Debug("using single chunk as configured - processing all documents in one range")
		// For a single chunk, we want to include all documents from beginning to end
		return []IdRange{{startID: primitive.NilObjectID, endID: primitive.NilObjectID}}, nil
	}

	//10k is more than enough even for hundreds of millions of documents
	//this is because we dont need a lot of chunks (max 10)
	sampleSize := 10000

	logger.Debug("sampling %d documents to determine chunk boundaries", sampleSize)

	// Get sample of _id values
	pipeline := mongo.Pipeline{
		{{Key: "$sample", Value: bson.D{{Key: "size", Value: sampleSize}}}},
		{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
		{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}}}},
	}

	cursor, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		logger.Error("failed to get sample ids: %v", err)
		return nil, fmt.Errorf("failed to get sample ids: %v", err)
	}
	defer cursor.Close(context.Background())

	// Collect sample IDs and deduplicate them
	var samples []interface{}
	seen := make(map[string]bool)

	for cursor.Next(context.Background()) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			logger.Error("failed to decode sample document: %v", err)
			return nil, fmt.Errorf("failed to decode sample document: %v", err)
		}

		// Deduplicate IDs by converting to string representation
		idVal := doc["_id"]
		idStr := fmt.Sprintf("%v", idVal) // Convert any ID type to string for deduplication
		if !seen[idStr] {
			seen[idStr] = true
			samples = append(samples, idVal)
		}
	}

	if len(samples) == 0 {
		logger.Debug("no samples found, using single chunk")
		return []IdRange{{startID: primitive.NilObjectID, endID: primitive.NilObjectID}}, nil
	}

	// Check if we have enough unique IDs for the requested chunks
	if len(samples) < chunks+1 {
		logger.Warn("not enough unique ids for requested chunks (%d), using %d chunks instead",
			chunks, len(samples)-1)
		chunks = len(samples) - 1
		if chunks <= 0 {
			logger.Debug("falling back to single chunk due to insufficient unique ids")
			return []IdRange{{startID: primitive.NilObjectID, endID: primitive.NilObjectID}}, nil
		}
	}

	logger.Debug("calculating split points from %d unique samples", len(samples))

	// Calculate split points
	splitPoints := make([]interface{}, 0, chunks-1)
	for i := 1; i < chunks; i++ {
		idx := (i * len(samples)) / chunks
		if idx >= len(samples) {
			idx = len(samples) - 1
		}
		splitPoints = append(splitPoints, samples[idx])
	}

	// Create ranges
	ranges := make([]IdRange, 0, chunks)
	var startID primitive.ObjectID = primitive.NilObjectID

	for _, endID := range splitPoints {
		// Convert interface{} to primitive.ObjectID
		endObjID, ok := endID.(primitive.ObjectID)
		if !ok {
			logger.Error("failed to convert endid to objectid")
			return nil, fmt.Errorf("failed to convert endid to objectid")
		}

		ranges = append(ranges, IdRange{startID: startID, endID: endObjID})
		startID = endObjID
	}
	ranges = append(ranges, IdRange{startID: startID, endID: primitive.NilObjectID})

	logger.Debug("created %d id ranges for chunking", len(ranges))
	return ranges, nil
}

// GetIDRanges3 calculates ID ranges for chunking by counting documents and iterating through them
// It counts all documents first, then iterates through the collection to find _id values at each percentage interval
func (m *MongoStorage) GetIDRanges3(collection *mongo.Collection, chunks int) ([]IdRange, error) {
	// If we're configured to use only 1 chunk, return a single range from first to last document
	singleRange := []IdRange{{startID: primitive.NilObjectID, endID: primitive.NilObjectID}}
	if chunks == 1 {
		logger.Debug("using single chunk as configured - processing all documents in one range")
		return singleRange, nil
	}

	// First count all documents in the collection
	logger.Debug("counting documents in collection %s", collection.Name())
	totalDocs, err := collection.CountDocuments(context.Background(), bson.M{})
	if err != nil {
		logger.Error("failed to count documents in collection: %v", err)
		return nil, fmt.Errorf("failed to count documents: %v", err)
	}
	if totalDocs < 1 {
		logger.Error("collection has no documents")
		return nil, fmt.Errorf("collection has no documents")
	}

	if totalDocs < 1000 {
		logger.Warn("collection has less than 1000 documents, using single chunk")
		return singleRange, nil
	}

	logger.Info("found %d total documents, splitting into %d chunks", totalDocs, chunks)

	// Calculate the document count per chunk
	docsPerChunk := totalDocs / int64(chunks)
	if docsPerChunk == 0 {
		docsPerChunk = 1
	}

	// Create cursor to iterate through all documents
	cursor, err := collection.Find(context.Background(), bson.M{}, options.Find().SetSort(bson.M{"_id": 1}))
	if err != nil {
		logger.Error("failed to create cursor for collection: %v", err)
		return nil, fmt.Errorf("failed to create cursor: %v", err)
	}
	defer cursor.Close(context.Background())

	// We need chunks-1 split points to create chunks ranges
	var splitPoints []primitive.ObjectID
	var currentCount int64
	var nextThreshold int64 = docsPerChunk
	maxSplitPoints := chunks - 1

	// Iterate through documents and record _id at each threshold
	for cursor.Next(context.Background()) {
		currentCount++

		if currentCount%500000 == 0 {
			logger.Debug("[ongoing GetIDRanges3] processed %d documents", currentCount)
		}

		// If we've reached the next threshold and haven't collected all needed split points
		if currentCount >= nextThreshold && len(splitPoints) < maxSplitPoints {
			var doc struct {
				ID primitive.ObjectID `bson:"_id"`
			}
			if err := cursor.Decode(&doc); err != nil {
				logger.Error("failed to decode document: %v", err)
				return nil, fmt.Errorf("failed to decode document: %v", err)
			}
			splitPoints = append(splitPoints, doc.ID)
			nextThreshold += docsPerChunk
		}
	}

	if err := cursor.Err(); err != nil {
		logger.Error("cursor error: %v", err)
		return nil, fmt.Errorf("cursor error: %v", err)
	}

	// Create ranges from split points
	ranges := make([]IdRange, 0, chunks)
	var startID primitive.ObjectID = primitive.NilObjectID

	// Create N ranges using N-1 split points
	for _, endID := range splitPoints {
		ranges = append(ranges, IdRange{startID: startID, endID: endID})
		startID = endID
	}
	// Add final range (from last split point to Nil)
	ranges = append(ranges, IdRange{startID: startID, endID: primitive.NilObjectID})

	logger.Debug("created %d id ranges for chunking", len(ranges))
	return ranges, nil
}

func loadSchema(tablePath string) (map[string]string, error) {
	logger.Info("loading schema for table: %s", tablePath)
	data, err := os.ReadFile(tablePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %v", err)
	}

	var schemaFields []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &schemaFields); err != nil {
		return nil, fmt.Errorf("failed to parse schema: %v", err)
	}

	fields := make(map[string]string)
	for _, field := range schemaFields {
		fields[field.Name] = field.Type
	}

	return fields, nil
}

// Helper function to build filter based on ID range
func buildFilter(r IdRange) bson.M {
	filter := bson.M{}

	// Handle special case for nil IDs (first and last chunks)
	if r.GetStartID() == primitive.NilObjectID && r.GetEndID() == primitive.NilObjectID {
		// No filter means all documents
		return filter
	}

	if r.GetStartID() != primitive.NilObjectID {
		filter["_id"] = bson.M{"$gte": r.GetStartID()}
	}

	if r.GetEndID() != primitive.NilObjectID {
		if _, exists := filter["_id"]; exists {
			// If we already have a start condition, add the end condition to it
			filter["_id"].(bson.M)["$lt"] = r.GetEndID()
		} else {
			// Otherwise just set the end condition
			filter["_id"] = bson.M{"$lt": r.GetEndID()}
		}
	}

	// Log the filter and ID ranges
	startIDStr := "nil"
	endIDStr := "nil"

	if r.GetStartID() != primitive.NilObjectID {
		startIDStr = r.GetStartID().Hex()
	}

	if r.GetEndID() != primitive.NilObjectID {
		endIDStr = r.GetEndID().Hex()
	}

	logger.Debug("built filter: %+v for range startid=%v, endid=%v",
		filter, startIDStr, endIDStr)

	return filter
}

// CountingWriter wraps an io.Writer and counts the bytes written
type CountingWriter struct {
	W     io.Writer
	Count int64
}

func (cw *CountingWriter) Write(p []byte) (int, error) {
	n, err := cw.W.Write(p)
	cw.Count += int64(n)
	return n, err
}

// processChunk processes a single chunk of documents and writes them to a gzipped JSONL file
// It will split files if they exceed n-GB compressed size
func (m *MongoStorage) processChunk(collection *mongo.Collection, tableName string, chunkNum int, r IdRange) ([]string, error) {
	// Generate base filename with timestamp and chunk number
	timestamp := time.Now().Format("20060102-1504")

	// Track all filenames created for this chunk
	var filenames []string

	// Find documents in this chunk
	filter := buildFilter(r)
	logger.Debug("chunk %d filter: %+v", chunkNum, filter)

	cursor, err := collection.Find(context.Background(), filter)
	if err != nil {
		logger.Error("failed to query documents for chunk %d: %v", chunkNum, err)
		return nil, fmt.Errorf("failed to query documents: %v", err)
	}
	defer cursor.Close(context.Background())

	// Process documents
	var docCount, totalDocCount, partNum int
	var file *os.File
	var gzWriter *gzip.Writer
	var countingWriter *CountingWriter

	// Create first output file
	filename := filepath.Join(m.tempDir, fmt.Sprintf("%s_%s_c%d_p%d.jsonl.gz", tableName, timestamp, chunkNum, partNum))
	logger.Debug("processing chunk %d to file: %s", chunkNum, filename)

	file, err = os.Create(filename)
	if err != nil {
		logger.Error("failed to create output file %s: %v", filename, err)
		return nil, fmt.Errorf("failed to create output file: %v", err)
	}

	// Add to filenames list
	filenames = append(filenames, filename)

	// Setup counting writer and gzip writer
	countingWriter = &CountingWriter{W: file}
	gzWriter = gzip.NewWriter(countingWriter)

	for cursor.Next(context.Background()) {
		// Check if we need to create a new file due to size limit
		if countingWriter.Count >= m.maxGzipFileSize && docCount > 0 {
			// Close current writers
			gzWriter.Close()
			file.Close()

			// Log completion of current part
			logger.Info("completed chunk %d part %d with %d documents, saved to %s (size: %d bytes)",
				chunkNum, partNum, docCount, filename, countingWriter.Count)

			// Create new file for next part
			partNum++
			docCount = 0
			filename = filepath.Join(m.tempDir, fmt.Sprintf("%s_%s_c%d_p%d.jsonl.gz", tableName, timestamp, chunkNum, partNum))
			logger.Debug("creating new file for chunk %d part %d: %s", chunkNum, partNum, filename)

			file, err = os.Create(filename)
			if err != nil {
				logger.Error("failed to create output file %s: %v", filename, err)
				return filenames, fmt.Errorf("failed to create output file: %v", err)
			}

			// Add to filenames list
			filenames = append(filenames, filename)

			// Setup new counting writer and gzip writer
			countingWriter = &CountingWriter{W: file}
			gzWriter = gzip.NewWriter(countingWriter)
		}

		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			logger.Error("failed to decode document in chunk %d: %v", chunkNum, err)
			continue
		}
		// logger.Debug("decoded document in chunk %d: %v", chunkNum, doc)

		// Rename _id to id for output
		if id, exists := doc["_id"]; exists {
			doc["id"] = id
			delete(doc, "_id")
		}

		//This is special case handling. For backward compatibility.
		//Somehow embulk outputs record as string, if the schema says so.
		for fieldName, value := range doc {

			// Remove null fields
			// Sometimes data from mongo has null value but schema says REPEATED
			if value == nil {
				delete(doc, fieldName)
				continue
			}

			// Handle [null] case - more robust type checking
			switch v := value.(type) {
			case primitive.A: // MongoDB's array type
				if len(v) == 1 && v[0] == nil {
					delete(doc, fieldName)
					continue
				}
			case []interface{}:
				if len(v) == 1 && v[0] == nil {
					delete(doc, fieldName)
					continue
				}
			}

			//special case, meta field type if STRING but filled with record{} :pepesad
			if fieldType, exists := m.fields[fieldName]; exists {
				if fieldType == "STRING" {
					switch value.(type) {
					case map[string]interface{}, bson.M:
						// Convert record to JSON string
						jsonBytes, err := json.Marshal(value)
						if err != nil {
							logger.Warn("failed to marshal record field '%s' to JSON: %v", fieldName, err)
							doc[fieldName] = "{}" // Default empty JSON object
						} else {
							doc[fieldName] = string(jsonBytes)
						}
					}
				}
			}
		}

		// Convert BSON to JSON
		jsonData, err := json.Marshal(doc)
		if err != nil {
			logger.Error("failed to marshal document in chunk %d: %v", chunkNum, err)
			continue
		}

		// Write JSON line to gzipped file
		if _, err := gzWriter.Write(append(jsonData, '\n')); err != nil {
			logger.Error("failed to write to gzip writer in chunk %d: %v", chunkNum, err)
			return filenames, fmt.Errorf("failed to write to gzip writer: %v", err)
		}

		//default gzwriter as part of `compress/flate/deflate.go` buffer is 512 bytes
		//we need to flush the writer to ensure the CountingWriter gets accurate size
		//flush every 10 docs
		if totalDocCount%10 == 0 {
			atomic.AddInt64(&m.processedDocCount, 10)
			if err := gzWriter.Flush(); err != nil {
				logger.Error("failed to flush gzip writer in chunk %d: %v", chunkNum, err)
				return filenames, fmt.Errorf("failed to flush gzip writer: %v", err)
			}
		}

		docCount++
		totalDocCount++

		// Log progress every X documents
		if totalDocCount%500000 == 0 {
			processedDocCount := atomic.LoadInt64(&m.processedDocCount)
			logger.Info("[ongoing] %s has %d documents. Total so far for this collection ~ %d",
				filename, docCount, processedDocCount)
		}
	}

	if err := cursor.Err(); err != nil {
		logger.Error("cursor error in chunk %d: %v", chunkNum, err)
		return filenames, fmt.Errorf("cursor error: %v", err)
	}

	// Close the last file
	gzWriter.Close()
	file.Close()

	if totalDocCount < 1 {
		return filenames, fmt.Errorf("no documents found in chunk %d for table %s", chunkNum, tableName)
	}

	logger.Info("completed chunk %d part %d with %d documents, saved to %s (size: %d bytes)",
		chunkNum, partNum, docCount, filename, countingWriter.Count)
	logger.Info("completed all parts of chunk %d with %d total documents across %d files", chunkNum, totalDocCount, len(filenames))

	return filenames, nil
}

// ReadIDRangesFromFile reads ID ranges from a JSON file
// The file should be an array of objects with optional "start_id" and "end_id" fields
// Returns a slice of IdRange and any error encountered
func (m *MongoStorage) ReadIDRangesFromFile(filePath string) ([]IdRange, error) {
	// Read the file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read ranges file: %v", err)
	}

	type jsonRange struct {
		StartID string `json:"start_id"`
		EndID   string `json:"end_id"`
	}

	var jsonRanges []jsonRange
	if err := json.Unmarshal(data, &jsonRanges); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ranges: %v", err)
	}

	ranges := make([]IdRange, 0, len(jsonRanges))
	for i, r := range jsonRanges {
		var startID, endID primitive.ObjectID
		var err error

		if i > 0 {
			startID, err = primitive.ObjectIDFromHex(r.StartID)
			if err != nil {
				return nil, fmt.Errorf("invalid start_id at index %d: %v", i, err)
			}
		} else {
			startID = primitive.NilObjectID
		}

		if i < len(jsonRanges)-1 {
			endID, err = primitive.ObjectIDFromHex(r.EndID)
			if err != nil {
				return nil, fmt.Errorf("invalid end_id at index %d: %v", i, err)
			}
		} else {
			endID = primitive.NilObjectID
		}

		ranges = append(ranges, IdRange{
			startID: startID,
			endID:   endID,
		})
	}

	return ranges, nil
}
