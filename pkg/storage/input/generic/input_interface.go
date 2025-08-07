package generic

// InputStorage defines the interface for input storage implementations
type InputStorage interface {
	// Setup initializes the input storage with configuration
	// config: input configuration from the parsed JSON
	Setup(config map[string]interface{}) error

	// Close cleans up the input storage resources
	Close() error

	// Read retrieves data for the specified table
	// tablePath: path of the table/collection to read from (json)
	// Returns: slice of file paths containing the exported data
	Read(tablePath string) ([]string, error)
}
