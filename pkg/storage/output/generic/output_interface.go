package generic

// OutputStorage defines the interface for output storage implementations
type OutputStorage interface {
	// Setup initializes the output storage with configuration
	// config: output configuration from the parsed JSON
	Setup(config map[string]interface{})

	// Close cleans up the output storage resources
	Close() error

	// Check if table exists already
	CheckTableExists(tablePath string) (bool, string, error)

	// Processes the files (uploads them) to target DB
	Write(tablePath string, files []string, overwriteTable bool) error
}
