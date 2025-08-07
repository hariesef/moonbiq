package main

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"

	"moonbiq/internal/storage/input/mongo"
	"moonbiq/pkg/logger"
)

func main() {
	outputPath := flag.String("output", "", "Path to output file (required)")
	method := flag.Int("method", 1, "Method to use for ID ranges (1: sampling, 2: time, 3: count)")
	chunks := flag.Int("chunks", 10, "Number of chunks to create")
	flag.Parse()

	if *outputPath == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	storage := mongo.New()
	logger.Initialize("debug")
	logger.Info("setting up database connection.")
	storage.Setup(map[string]interface{}{
		"chunks": float64(*chunks),
	})
	defer storage.Close()

	base := filepath.Base(*outputPath)
	tableName := strings.TrimSuffix(base, filepath.Ext(base))
	logger.Info("mongo table name to read: %s", tableName)

	collection := storage.GetCollection(tableName)
	logger.Info("getting ID ranges with method %d", *method)

	var ranges []mongo.IdRange
	var err error
	switch *method {
	case 1:
		ranges, err = storage.GetIDRanges(collection, *chunks)
	case 2:
		ranges, err = storage.GetIDRanges2(collection, *chunks)
	case 3:
		ranges, err = storage.GetIDRanges3(collection, *chunks)
	default:
		logger.Error("invalid method: %d", *method)
		os.Exit(1)
	}
	if err != nil {
		logger.Error("failed to get id ranges: %v", err)
		os.Exit(1)
	}
	logger.Info("got %d id ranges", ranges)

	// Create a serializable structure for the ranges
	type serializableIDRange struct {
		StartID string `json:"start_id,omitempty"`
		EndID   string `json:"end_id,omitempty"`
	}

	// Convert idRange to serializable format
	serializableRanges := make([]serializableIDRange, 0, len(ranges))
	for i, r := range ranges {
		rangeItem := serializableIDRange{}

		// For all ranges except the first, include start_id
		if i > 0 {
			rangeItem.StartID = r.GetStartID().Hex()
		}

		// For all ranges except the last, include end_id
		if i < len(ranges)-1 {
			rangeItem.EndID = r.GetEndID().Hex()
		}

		serializableRanges = append(serializableRanges, rangeItem)
	}

	// Convert to JSON for file output (2-space indentation)
	rangesJSON, err := json.MarshalIndent(serializableRanges, "", "  ")
	if err != nil {
		logger.Error("failed to marshal id ranges: %v", err)
		os.Exit(1)
	}
	logger.Info("ID Ranges:\n%s", rangesJSON)

	// Write to output file
	if err := os.WriteFile(*outputPath, rangesJSON, 0644); err != nil {
		logger.Error("failed to write id ranges to file: %v", err)
		os.Exit(1)
	}

	logger.Info("successfully wrote %d id ranges to %s", len(ranges), *outputPath)
}
