package moonbiq

import (
	"moonbiq/internal/storage/input/mongo"
	"moonbiq/internal/storage/output/bigquery"
	"moonbiq/pkg/logger"
	ig "moonbiq/pkg/storage/input/generic"
	og "moonbiq/pkg/storage/output/generic"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Service struct {
	inputStorage   ig.InputStorage
	inputConfig    map[string]interface{}
	outputStorage  og.OutputStorage
	outputConfig   map[string]interface{}
	tablePath      string
	wg             *sync.WaitGroup
	overwriteTable bool
}

// New creates a new moonbiq service instance
func New(inputConfig map[string]interface{}, outputConfig map[string]interface{},
	tablePath string, overwriteTable bool) *Service {

	// Get table name from file name (without .json extension)
	tableName := filepath.Base(tablePath)
	tableName = strings.TrimSuffix(tableName, ".json")
	if tableName == "" {
		logger.Error("could not determine table name from config file")
		os.Exit(1)
	}

	// Get input type from config
	inputType, ok := inputConfig["type"].(string)
	if !ok {
		logger.Error("input type not specified in config")
		os.Exit(1)
	}

	// Get output type from config
	outputType, ok := outputConfig["type"].(string)
	if !ok {
		logger.Error("output type not specified in config")
		os.Exit(1)
	}

	// Initialize input storage based on type
	var inputStorage ig.InputStorage
	switch inputType {
	case "mongo":
		// Initialize storage with config
		storage := mongo.New()
		if err := storage.Setup(inputConfig); err != nil {
			logger.Error("failed to setup storage: %v", err)
			os.Exit(1)
		}
		inputStorage = storage
	default:
		logger.Error("unsupported input type: %s", inputType)
		os.Exit(1)
	}

	// Initialize output storage
	var outputStorage og.OutputStorage
	switch outputType {
	case "bigquery":
		// Initialize storage with config
		storage := bigquery.New()
		storage.Setup(outputConfig)
		outputStorage = storage
	default:
		logger.Error("unsupported output type: %s", outputType)
		os.Exit(1)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	return &Service{
		inputStorage:   inputStorage,
		inputConfig:    inputConfig,
		outputStorage:  outputStorage,
		outputConfig:   outputConfig,
		tablePath:      tablePath,
		wg:             wg,
		overwriteTable: overwriteTable,
	}
}

func (s *Service) GetWaitGroup() *sync.WaitGroup {
	return s.wg
}

// Run executes the moonbiq service
func (s *Service) Run() {
	defer s.wg.Done()

	logger.Info("starting moonbiq service for table: %s", s.tablePath)

	// Initialize moonbiq service
	if s.inputStorage == nil {
		logger.Error("failed to create input storage")
		os.Exit(1)
	}

	exists, normalizedName, err := s.outputStorage.CheckTableExists(s.tablePath)
	if err != nil {
		logger.Error("failed to check table existence: %v", err)
		os.Exit(1)
	}
	if exists && !s.overwriteTable {
		logger.Info("table %s already exists, skipping (use -overwrite to force)", normalizedName)
		os.Exit(0)
	}

	// Call input storage's Read() method
	logger.Info("reading collection: %s", s.tablePath)
	files, err := s.inputStorage.Read(s.tablePath)
	if err != nil {
		logger.Error("error reading input table: %v", err)
		os.Exit(1)
	}
	logger.Info("input plugin successfully generated %d files", len(files))

	// Call output storage's Write() method
	logger.Info("importing generated files of schema %s to output plugin.", s.tablePath)
	err = s.outputStorage.Write(s.tablePath, files, s.overwriteTable)
	if err != nil {
		logger.Error("error importing table data: %v", err)
		os.Exit(1)
	}
	logger.Info("output plugin successfully imported all files.")

	// Close the input storage
	logger.Debug("closing input storage")
	err = s.inputStorage.Close()
	if err != nil {
		logger.Error("error closing input storage: %v", err)
	}
	// Close the output storage
	logger.Debug("closing output storage")
	err = s.outputStorage.Close()
	if err != nil {
		logger.Error("error closing output storage: %v", err)
	}
	logger.Debug("closed input and output storage")
}
