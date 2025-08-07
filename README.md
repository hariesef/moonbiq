# moonbiQ
Inspired by embulk (https://www.embulk.org/), Moonbiq is a high-performance data pipeline tool developed in Go for exporting data from MongoDB to BigQuery. While embulk only supports one table at a time, moonbiq also features parallel tables processing capabilities through moonbatch mode.  Following embulk architecture, moonbiq was developed with plugins concept in mind. Means any input/ output source and target of database is just step away for development and to be integrated with.

## Key Features
-  **Parallel Processing**: Processes collections concurrently using goroutines (via moonbatch)
-  **Modular Architecture**: Clean separation between input/output plugins
-  **Configuration Driven**: JSON configuration for all components
-  **Robust Error Handling**: Comprehensive logging and error recovery
-  **Performance Optimized**: Chunked processing and parallel uploads

  

## Preparation 
### Building Binaries
Ensure you have Go >= 1.23 properly setup.
```bash
go mod tidy
./build
```
This will generate three binaries that will be explained later:  **moonbiq**, **moonbatch** and **getsampling**.

### Preparing Input Plugin Configuration
Example configuration exists on **config/input/mongo.json**
```
{
  "type": "mongo",
  "chunks": 10,
  "maxGzipFileSize": 4096000000,
  "rangesSplitMethod": "sampling"
}
```  
Input/Output confguration is free-form json files that can be adjusted to what needed by the plugins.  For mongo input plugin that is native from moonbiq, here is the description:
 - **type** is hardcoded convention of input plugin. It is used to identify (on source code) what plugin package shall be initialized.
 - **chunks** represents number of parallel threads for downloading data from table, into offline temporary files. In example, specifying this to 3 will create three separate output files which are handled by separate, parallel own thread.  As per current release, we are limiting max chunks to 10 so it won't put too much I/O burden to both disk or database during parallel table operations.
 - **maxGzipFileSize** In case one chunk output file has size more than certain size,  the input plugin will close current gzip file and open another part. This is especially important for big query import that only supports max 4.3GB size per GZip file. In accordance to output filename convention for chunk and part (_cX_pX), it will look like this `video_catalog.sources_20250804-2151_c5_p0.jsonl.gz`.
 - rangesSplitMethod valid values are 
	- "sampling"
	- "time"
	- "count"

In order to have parallel export of single table/ collection, mongo input plugin needs to know what will be beginning and end criteria of rows/collections it should be working on. By using the main key _id ObjectId on mongo collection, there can be three ways to determine the split _id range:
1. "**count**" will count number of documents inside collection first. And then will go through for each document and mark the proportional _id according to chunk number. For example if table has 1000 documents and number of chunk is 5,  then we will have five ranges, each with _id from document [1st to 200th], [201 to 400], [401 to 600], [601 to 800] and  [801 to 1000]. This is the most classic, conservative way to do range split and the slowest one. But it ensures each thread is given equal amount of documents to process.
2. "**time**" will first count total documents, just like count,  but it will not go through doc one by one. Instead it will get the time delta between 1000th doc and the first one. This is utilizing the fact that ObjectId in mongo actually contains time information. Then by using proportional value of the time delta, we can approximate the objectId that falls under 20%, 40%, 60% and 80% of delta, to build five ranges.  Using this method is faster than count, has biggest drawback when the document population on certain period is considerably many more than other period. Then in that case, the amount of documents discrepancy between thread/ chunk will be huge.
3. "**sampling**" does not do document count beforehand. It utilizes mongo pipeline `$sample` ability to randomly get documents from collection. Currently the number of samples to retrieved is fixed at 10,000 docs.  The retrieved samples will then be sorted, and then using proportional value, the five ranges will be determined easily. This method is the quickest one. Although will not guarantee that each thread will be given fair amount of docs, based on the tests result, the normal distribution is relatively acceptable.

Aside from the input file, an environment variable is also needed by mongo input plugin. 
```bash
export MONGO_URI="mongodb://user:password@localhost:27007/database-name?authSource=database-name&readPreference=primary&directConnection=true&ssl=false"  
```
Current input plugin only uses single MONGO_URI for authentication, and uses the database name inside the URI.

### Preparing Input/Output Table Configuration
Example can be seen from **config/table/example/users.json**. The format follows BigQuery table creation convention, and this is pretty much universal can be applied to practically, any database. 
**Why It’s Quite Universal:**
BigQuery uses a **JSON schema format** that supports:
-   **Primitive types**: `STRING`, `INTEGER`, `FLOAT`, `BOOLEAN`, `BYTES`, `DATE`, `TIME`, `TIMESTAMP`, etc. 
-   **Nested fields (RECORD/ STRUCT)**: You can have fields with their own subfields.  
-   **Repeated fields (ARRAY)**: You can have lists of values or records.    
-   **Null-able, required, repeated** modes.    
These features allow you to represent complex structures like:
-   SQL tables from traditional RDBMS (PostgreSQL, MySQL, Oracle, etc.)  
-   NoSQL documents (like MongoDB or Firestore documents)

Although the table configuration is mainly needed by output plugin/ BQ to create table, the input plugin still needs it to deal with erroneous data inside from the mongo DB.  Based on test results (we are not discussing why/ what was the cause of invalid data), there could be these cases:
- The schema type is REPEATED (array), but the value from mongo is null. Empty array [] is acceptable by BQ, but null is not.
- The schema type is REPEATED but it contains null, i.e.  [null]
- This one is very specific: in mongo the type if struct, but we want to merge them into single string in BQ (the field is defined as STRING in schema file).
Since all of above handling was supported by Embulk, hence moonbiq also implemented them.

**Special notes:**
- Moonbiq uses filename convention to determine the table/ collection name. Say, if table file configuration is `./config/table/video_catalog.sources.json`  then it will take **video_catalog.sources** as table name. However in BQ this will be normalized to **video_catalog_sources** since BQ does not accept special character except underscore.

### Preparing Output Plugin Configuration
Example configuration exists on **config/output/bigquery.json**
```
{
  "type": "bigquery",
  "projectId": "gcp-project-id",
  "useTempDataset": true,
  "tempDataSet": "temp",
  "finalDataset": "master",
  "gcsBucket": "temp-mongo-bq",
  "gcsParallelUploads": 10
}
```
Explanation:

 - "**type**" is the hardcoded convention for output BigQuery. 
 - "**projectId**" the Google Cloud Platform project to use.
 - "**useTempDataset**" in case true,  the table will be created first inside **tempDataset** and data will be uploaded there. Upon completion,  the table will be copied to **finalDataset** (truncate/ override). This method is preferable so that during upload process (which may take a while), the final table can still be used/ queried by users.
 - "**gcsBucket**" the bucket name, to upload gzip files produced by mongo input plugin. Moonbiq BQ plugin uses the GCS method to upload data, via data-transfer-job function in big query. This way the file upload can be parallel in GCS without concern to meet rate limit in BQ API. After all files are successfully uploaded, the plugin will start the data transfer job to load files from GCS bucket, and wait until it completes.
 - "**gcsParallelUploads**" how many parallel threads to upload files to GCS bucket.

**Special notes:**
To authenticate with Google cloud, BQ output plugin uses this environment variable that points to service account key:
```
export GOOGLE_APPLICATION_CREDENTIALS="./service-account.json"  
```

## Executing
### Single Table/ Collection
After building the binary, we can run `./moonbiq -help` to display the available options:
```
Usage of ./moonbiq:
  -input string
    	Path to input configuration file (default "config/input/mongo.json")
  -loglevel string
    	Logging level (debug, info, warn, error) (default "info")
  -output string
    	Path to output configuration file (default "config/output/bigquery.json")
  -overwrite
    	If table already exists on target DB, overwrite it.
  -table string
    	Path to table configuration file (required)
```
moonbiq requires temporary folder to place the offline files, prepare the folder beforehand:
```
mkdir temp
export MOONBIQ_TEMP="./temp"
```
Example in how to run:
```
./moonbiq -input config/input/mongo.json -output config/output/bigquery.json -table config/table/example/users.json -overwrite -loglevel debug
```
Before running please set the previously mentioned environment variables.

### Multiple Tables/ Batch mode
First of all, put all of the table json configuration files into a single folder. Then we can run `./moonbatch -help` to see the available options:
```
Usage of ./moonbatch:
  -binarypath string
    	Path to moonbiq binary (default "./moonbiq")
  -input string
    	Path to input configuration file (default "config/input/mongo.json")
  -loglevel string
    	Sending -loglevel to moonbiq (default "info")
  -output string
    	Path to output configuration file (default "config/output/bigquery.json")
  -overwrite
    	Sending -overwrite to moonbiq
  -targetfolder string
    	Target folder containing table JSON files (default "config/table")
  -workers int
    	Number of worker threads (default 3)
```
Minimum parameter to run moonbatch:
```
./moonbatch -targetfolder ./config/table 
```  
- Moonbatch will executing moonbiq binary for every table
- We can set how many tables are processed at same time, through `-workers` option.


### Improving moonbiQ Execution Performance
In order to dump table data efficiently, parallel threads processing i.e. `"chunks": 10` is required. However, creating table split range (via method count, time or sampling) may need a lot of time especially if table/ collection size is huge. To overcome this, it is possible to load the table split range, offline from a file.
```
Usage of ./getsampling:
  -chunks int
    	Number of chunks to create (default 10)
  -method int
    	Method to use for ID ranges (1: sampling, 2: time, 3: count) (default 1)
  -output string
    	Path to output file (required)
```
Recommended use:
```
./getsampling -chunks 10 -method 1 -output ./config/ranges/users.json
```
- **getsampling** is used to create table id ranges, and write them to output file.
- The number of chunks must be inline with configured chunks in `mongo.json`.
- Table name is still using the filename convention, i.e. in above example the table name to query from mongo is **users**.
- During execution, **moonbiq** binary will check the location of environment variable `MOONBIQ_RANGE_FILE_FOLDER` for the existence of file users.json. If found, it will read the table range from there.  Specify dedicated folder to store the ranges files, and store the getsampling outputs there.
```
export MOONBIQ_RANGE_FILE_FOLDER="/home/jenkins/moonbiq_ranges"
```
- Range files do not need to be updated daily, since it is very likely that new data will fall under last range. Recommended to run getsampling weekly or even monthly to refresh the ranges files.

### Performance Benchmarking
This is real environment test result, using three tables execution at once:
- table A has 302,038,888 rows
- table B has 99,320,378 rows
- table C has 531,049,063 rows

moonbiq completion time: **4h 32m**

embulk completion time: 6h 46m

Note: table ranges generated beforehand using sampling method. 

### Running Unit Tests
```
make go-gen
make test
```
- For mongo plugin the unit test is real/ online, using mongo docker (initialized inside the test). This approach is needed due to complexity of mongo collection handling, especially the id ranges testing and the output files generation.
- For big query the test is using mocks, that encapsulates actual network API like creating table in BQ dataset.


## Creating New Plugins
moonbiQ's modular architecture makes it easy to add support for new databases.
 
### Input Plugin Development
1. Create a new package under `internal/storage/input/`
2. Implement the `InputStorage` interface from `pkg/storage/input/generic`:
```go
// InputStorage defines the interface for input storage implementations
type  InputStorage  interface {
// Setup initializes the input storage with configuration
// config: input configuration from the parsed JSON
Setup(config  map[string]interface{}) error

// Close cleans up the input storage resources
Close() error

// Read retrieves data for the specified table
// tablePath: path of the table/collection to read from (json)
// Returns: slice of file paths containing the exported data
Read(tablePath  string) ([]string, error)
}
```
Key responsibilities:
- Extract data from source system
- Save to temporary files (JSONL format + Gzip)
- Return list of generated file paths

Example workflow:
1. Parse configuration in `Setup()`
2. In `Read()`, export data to temp files and return file paths for output plugin
3. `Close()` will be called by moonbiq core to close resources.

  

### Output Plugin Development
1. Create new package under `internal/storage/output/`
2. Implement the `OutputStorage` interface from `pkg/storage/output/generic`:
```go
// OutputStorage defines the interface for output storage implementations
type  OutputStorage  interface {

// Setup initializes the output storage with configuration
// config: output configuration from the parsed JSON
Setup(config  map[string]interface{})

// Close cleans up the output storage resources
Close() error

// Check if table exists already
CheckTableExists(tablePath  string) (bool, string, error)

// Processes the files (uploads them) to target DB
Write(tablePath  string, files []string, overwriteTable  bool) error
}
```
Key responsibilities:
- Validate input files
- Prepare target system (create tables if needed)
- Load data to target
- Handle cleanup as necessary

**Special Notes:**
- For the `config  map[string]interface{}`,  it actually is the content of input/output configuration, i.e. `config/input/mongo.json`. The type is generic so that the input/output plugin can freely describe the structure and use any field as needed.
- The folder where input plugin writes, and output plugin reads, is based on the following environment variable:
```
export MOONBIQ_TEMP="./temp"
```
  - the folder is not automatically created. therefore please ensure it exists.  

### Registration of Plugins
Add your new plugin to the factory method in:
-  `internal/services/moonbiq/moonbiq.go` (for both input and output plugins)

Add a new case in the input/ output type switch under function `New()` 
```go
// Initialize input storage based on type
var  inputStorage  ig.InputStorage
switch  inputType {
  case  "mongo":
  // Initialize storage with config
  storage  :=  mongo.New()
  if  err  :=  storage.Setup(inputConfig); err  !=  nil {
    logger.Error("failed to setup storage: %v", err)
    os.Exit(1)
  }
  inputStorage  =  storage
  ```

Aside from implementing the interface functions, one mandatory public constructur New() as in 
```
storage  :=  mongo.New()
```
is needed to get the initial object instance.


### Thank you for your interest in moonbiQ!
