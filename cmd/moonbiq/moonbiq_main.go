package main

import (
	"encoding/json"
	"flag"
	"os"
	"time"

	"moonbiq/internal/services/moonbiq"
	"moonbiq/pkg/logger"
)

func main() {

	// Parse command line arguments
	logLevel := flag.String("loglevel", "info", "Logging level (debug, info, warn, error)")
	inputPath := flag.String("input", "config/input/mongo.json", "Path to input configuration file")
	outputPath := flag.String("output", "config/output/bigquery.json", "Path to output configuration file")
	tablePath := flag.String("table", "", "Path to table configuration file (required)")
	overwriteTable := flag.Bool("overwrite", false, "If table already exists on target DB, overwrite it.")
	flag.Parse()

	// Show help if table path is missing
	if *tablePath == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	logger.Initialize(*logLevel)

	// Validate configuration files
	if _, err := os.Stat(*inputPath); os.IsNotExist(err) {
		logger.Error("Input configuration file %s does not exist", *inputPath)
		os.Exit(1)
	}

	if _, err := os.Stat(*outputPath); os.IsNotExist(err) {
		logger.Error("Output configuration file %s does not exist", *outputPath)
		os.Exit(1)
	}

	if _, err := os.Stat(*tablePath); os.IsNotExist(err) {
		logger.Error("Table configuration file %s does not exist", *tablePath)
		os.Exit(1)
	}

	// Read input configuration
	inputConfigBytes, err := os.ReadFile(*inputPath)
	if err != nil {
		logger.Error("Error reading input config: %v", err)
		os.Exit(1)
	}
	var inputConfig map[string]interface{}
	if err := json.Unmarshal(inputConfigBytes, &inputConfig); err != nil {
		logger.Error("Error parsing input config: %v", err)
		os.Exit(1)
	}

	// Read output configuration
	outputConfigBytes, err := os.ReadFile(*outputPath)
	if err != nil {
		logger.Error("Error reading output config: %v", err)
		os.Exit(1)
	}
	var outputConfig map[string]interface{}
	if err := json.Unmarshal(outputConfigBytes, &outputConfig); err != nil {
		logger.Error("Error parsing output config: %v", err)
		os.Exit(1)
	}

	service := moonbiq.New(inputConfig, outputConfig, *tablePath, *overwriteTable)
	if service == nil {
		logger.Error("Failed to create moonbiq service")
		os.Exit(1)
	}

	// Record start time
	startTime := time.Now()
	// Run the service
	go service.Run()
	service.GetWaitGroup().Wait()

	// Calculate and print execution time
	executionTime := time.Since(startTime)
	logger.Info("Processing table %s completed in %s", *tablePath, executionTime)
	logger.Info("Moonbiq completed successfully")
}
