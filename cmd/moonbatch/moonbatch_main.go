package main

/*
This is to run moonbiq cmd binary in batch mode.
*/
import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type CmdParams struct {
	BinaryPath   string
	TargetFolder string
	Overwrite    bool
	Loglevel     string
	InputPath    string
	OutputPath   string
}

// ProcessResult represents the result of processing a collection
type ProcessResult struct {
	Collection string
	Success    bool
	Error      error
	Duration   time.Duration
}

// TableInfo represents information about a BigQuery table
type TableInfo struct {
	ID string `json:"id"`
}

func main() {
	cmdParams := &CmdParams{}
	// Parse command-line flags
	targetFolder := flag.String("targetfolder", "config/table", "Target folder containing table JSON files")
	binaryPath := flag.String("binarypath", "./moonbiq", "Path to moonbiq binary")
	numWorkers := flag.Int("workers", 3, "Number of worker threads")
	overwrite := flag.Bool("overwrite", false, "Sending -overwrite to moonbiq")
	loglevel := flag.String("loglevel", "info", "Sending -loglevel to moonbiq")
	inputPath := flag.String("input", "config/input/mongo.json", "Path to input configuration file")
	outputPath := flag.String("output", "config/output/bigquery.json", "Path to output configuration file")
	flag.Parse()

	// Assign parsed values
	cmdParams.TargetFolder = *targetFolder
	cmdParams.BinaryPath = *binaryPath
	cmdParams.Overwrite = *overwrite
	cmdParams.Loglevel = *loglevel
	cmdParams.InputPath = *inputPath
	cmdParams.OutputPath = *outputPath

	// Validate inputs
	if cmdParams.TargetFolder == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
	// Remove trailing slash from targetFolder if present
	cmdParams.TargetFolder = strings.TrimSuffix(cmdParams.TargetFolder, "/")

	// Find collections to process
	var collections []string
	var err error

	// Find all JSON files in the target folder
	collections, err = findCollections(cmdParams.TargetFolder)
	if err != nil {
		log.Fatalf("Error finding collections: %v", err)
	}

	if len(collections) == 0 {
		log.Fatal("No collections found in target folder")
	}

	fmt.Printf("Found %d collections to process\n", len(collections))

	// Create a channel to send collections to workers
	collectionChan := make(chan string, len(collections))

	// Create a channel to receive results from workers
	resultChan := make(chan ProcessResult, len(collections))

	// Create a wait group to wait for all workers to finish
	var wg sync.WaitGroup

	// Start worker goroutines
	workerCount := *numWorkers
	if workerCount > len(collections) {
		workerCount = len(collections)
	}

	fmt.Printf("Starting %d worker threads\n", workerCount)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(i, cmdParams, collectionChan, resultChan, &wg)
	}

	// Send collections to the channel
	for _, collection := range collections {
		collectionChan <- collection
	}
	close(collectionChan)

	// Wait for all workers to finish
	wg.Wait()

	// Close the results channel
	close(resultChan)

	// Process results
	var failedCollections []ProcessResult
	var successCount int
	var skipCount int

	for result := range resultChan {
		if result.Error != nil {
			failedCollections = append(failedCollections, result)
		} else if result.Success {
			successCount++
		} else {
			skipCount++
		}
	}

	// Print summary
	fmt.Printf("\nProcessing summary:\n")
	fmt.Printf("  Total collections: %d\n", len(collections))
	fmt.Printf("  Successfully processed: %d\n", successCount)
	fmt.Printf("  Skipped (already exist): %d\n", skipCount)
	fmt.Printf("  Failed: %d\n", len(failedCollections))

	if len(failedCollections) > 0 {
		fmt.Println("\nFailed collections:")
		for _, result := range failedCollections {
			fmt.Printf("  %s: %v\n", result.Collection, result.Error)
		}
		os.Exit(1)
	} else {
		fmt.Println("All collections processed successfully")
	}
}

// worker processes collections from the channel
func worker(id int, cmdParams *CmdParams, collections <-chan string,
	results chan<- ProcessResult, wg *sync.WaitGroup) {
	defer wg.Done()

	for collection := range collections {
		fmt.Printf("Worker %d processing collection: %s\n", id, collection)

		// Build the command to run the MongoDB export script through bundle exec
		cmdArgs := []string{
			"-table", collection,
			"-loglevel", cmdParams.Loglevel,
			"-input", cmdParams.InputPath,
			"-output", cmdParams.OutputPath,
		}
		if cmdParams.Overwrite {
			cmdArgs = append(cmdArgs, "-overwrite")
		}

		// Log the command and parameters
		fmt.Printf("Worker %d: Running command: %s %s\n", id, cmdParams.BinaryPath, strings.Join(cmdArgs, " "))

		cmd := exec.Command(cmdParams.BinaryPath, cmdArgs...)

		// Set the command to inherit stdout and stderr
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		// Run the command
		startTime := time.Now()
		err := cmd.Run()
		duration := time.Since(startTime)

		if err != nil {
			fmt.Printf("Worker %d: Error processing collection %s: %v (took %v)\n",
				id, collection, err, duration)
			// Send result for failed collection
			results <- ProcessResult{
				Collection: collection,
				Success:    false,
				Error:      err,
				Duration:   duration,
			}
		} else {
			fmt.Printf("Worker %d: Successfully processed collection %s (took %v)\n",
				id, collection, duration)
			// Send result for successful collection
			results <- ProcessResult{
				Collection: collection,
				Success:    true,
				Error:      nil,
				Duration:   duration,
			}
		}
	}
}

// findCollections finds all JSON files in the target folder
func findCollections(targetFolder string) ([]string, error) {

	// Check if the target folder exists
	if _, err := os.Stat(targetFolder); os.IsNotExist(err) {
		return nil, fmt.Errorf("target folder does not exist: %s", targetFolder)
	}

	// Find all JSON files in the target folder
	files, err := filepath.Glob(filepath.Join(targetFolder, "*.json"))
	if err != nil {
		return nil, err
	}

	return files, nil
}
