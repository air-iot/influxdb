// Package v8 contains code for importing data from 0.8 instances of InfluxDB.
package v8 // import "github.com/influxdata/influxdb/importer/v8"

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/influxdata/influxdb/client"
)

type Progress func(inserts int, failures int)

// Config is the config used to initialize a Importer importer
type Config struct {
	Path       string // Path to import data.
	Version    string
	Compressed bool // Whether import data is gzipped.
	PPS        int  // points per second importer imports with.
	DataOnly   bool // 文件中只包含数据, 不包含 DDL 和 DML 操作
	BatchSize  int
	client.Config
}

// NewConfig returns an initialized *Config
func NewConfig() Config {
	return Config{Config: client.NewConfig()}
}

// Importer is the importer used for importing 0.8 data
type Importer struct {
	client                *client.Client
	database              string
	retentionPolicy       string
	config                Config
	batchSize             int
	batch                 []string
	totalInserts          int
	failedInserts         int
	totalCommands         int
	throttlePointsWritten int
	startTime             time.Time
	lastWrite             time.Time
	throttle              *time.Ticker

	progress Progress

	stderrLogger *log.Logger
	stdoutLogger *log.Logger
}

// NewImporter will return an intialized Importer struct
func NewImporter(config Config) *Importer {
	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 5000
	}
	config.UserAgent = fmt.Sprintf("influxDB importer/%s", config.Version)
	return &Importer{
		config:       config,
		batchSize:    batchSize,
		batch:        make([]string, 0, batchSize),
		stdoutLogger: log.New(os.Stdout, "", log.LstdFlags),
		stderrLogger: log.New(os.Stderr, "", log.LstdFlags),
	}
}

func (i *Importer) Database(database string) {
	i.database = database
}

func (i *Importer) ImportStream(reader io.Reader, progress Progress) (inserts int, failed int, err error) {
	// Create a client and try to connect.
	cl, err := client.NewClient(i.config.Config)
	if err != nil {
		return 0, 0, fmt.Errorf("could not create client %s", err)
	}
	i.client = cl
	if _, _, e := i.client.Ping(); e != nil {
		return 0, 0, fmt.Errorf("failed to connect to %s\n", i.client.Addr())
	}

	i.progress = progress

	// Get our reader
	scanner := bufio.NewReader(reader)

	// 如果文件中只包含数据, 则不执行解析 DDL 动作
	// Process the DDL
	if !i.config.DataOnly {
		if err := i.processDDL(scanner); err != nil {
			return 0, 0, fmt.Errorf("reading standard input: %s", err)
		}
	}

	i.throttle = time.NewTicker(time.Microsecond)
	defer i.throttle.Stop()

	// Prime the last write
	i.lastWrite = time.Now()

	// Process the DML
	if err := i.processDML(scanner); err != nil {
		return i.totalInserts, i.failedInserts, fmt.Errorf("reading standard input: %s", err)
	}

	// If there were any failed inserts then return an error so that a non-zero
	// exit code can be returned.
	if i.failedInserts > 0 {
		plural := " was"
		if i.failedInserts > 1 {
			plural = "s were"
		}

		return i.totalInserts, i.failedInserts, fmt.Errorf("%d point%s not inserted", i.failedInserts, plural)
	}

	return i.totalInserts, i.failedInserts, nil
}

// Import processes the specified file in the Config and writes the data to the databases in chunks specified by batchSize
func (i *Importer) Import() error {
	// Create a client and try to connect.
	cl, err := client.NewClient(i.config.Config)
	if err != nil {
		return fmt.Errorf("could not create client %s", err)
	}
	i.client = cl
	if _, _, e := i.client.Ping(); e != nil {
		return fmt.Errorf("failed to connect to %s\n", i.client.Addr())
	}

	// Validate args
	if i.config.Path == "" {
		return fmt.Errorf("file argument required")
	}

	defer func() {
		if i.totalInserts > 0 {
			i.stdoutLogger.Printf("Processed %d commands\n", i.totalCommands)
			i.stdoutLogger.Printf("Processed %d inserts\n", i.totalInserts)
			i.stdoutLogger.Printf("Failed %d inserts\n", i.failedInserts)
		}
	}()

	// Open the file
	f, err := os.Open(i.config.Path)
	if err != nil {
		return err
	}
	defer f.Close()

	var r io.Reader

	// If gzipped, wrap in a gzip reader
	if i.config.Compressed {
		gr, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		defer gr.Close()

		//records, _ := binary.Varint(gr.Header.Extra)
		//fmt.Printf("total %d records\n", records)

		// Set the reader to the gzip reader
		r = gr
	} else {
		// Standard text file so our reader can just be the file
		r = f
	}

	// Get our reader
	scanner := bufio.NewReader(r)

	// 如果文件中只包含数据, 则不执行解析 DDL 动作
	// Process the DDL
	if !i.config.DataOnly {
		if err := i.processDDL(scanner); err != nil {
			return fmt.Errorf("reading standard input: %s", err)
		}
	}

	// Set up our throttle channel.  Since there is effectively no other activity at this point
	// the smaller resolution gets us much closer to the requested PPS
	i.throttle = time.NewTicker(time.Microsecond)
	defer i.throttle.Stop()

	// Prime the last write
	i.lastWrite = time.Now()

	// Process the DML
	if err := i.processDML(scanner); err != nil {
		return fmt.Errorf("reading standard input: %s", err)
	}

	// If there were any failed inserts then return an error so that a non-zero
	// exit code can be returned.
	if i.failedInserts > 0 {
		plural := " was"
		if i.failedInserts > 1 {
			plural = "s were"
		}

		return fmt.Errorf("%d point%s not inserted", i.failedInserts, plural)
	}

	return nil
}

func (i *Importer) processDDL(scanner *bufio.Reader) error {
	for {
		line, err := scanner.ReadString(byte('\n'))
		if err != nil && err != io.EOF {
			return err
		} else if err == io.EOF {
			return nil
		}
		// If we find the DML token, we are done with DDL
		if strings.HasPrefix(line, "# DML") {
			return nil
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		// Skip blank lines
		if strings.TrimSpace(line) == "" {
			continue
		}
		i.queryExecutor(line)
	}
}

func (i *Importer) processDML(scanner *bufio.Reader) error {
	i.startTime = time.Now()
	for {
		line, err := scanner.ReadString(byte('\n'))
		if err != nil && err != io.EOF {
			return err
		} else if err == io.EOF {
			// Call batchWrite one last time to flush anything out in the batch
			i.batchWrite()
			return nil
		}
		if strings.HasPrefix(line, "# CONTEXT-DATABASE:") {
			i.batchWrite()
			i.database = strings.TrimSpace(strings.Split(line, ":")[1])
		}
		if strings.HasPrefix(line, "# CONTEXT-RETENTION-POLICY:") {
			i.batchWrite()
			i.retentionPolicy = strings.TrimSpace(strings.Split(line, ":")[1])
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		// Skip blank lines
		if strings.TrimSpace(line) == "" {
			continue
		}
		i.batchAccumulator(line)
	}
}

func (i *Importer) execute(command string) {
	response, err := i.client.Query(client.Query{Command: command, Database: i.database})
	if err != nil {
		i.stderrLogger.Printf("error: %s\n", err)
		return
	}
	if err := response.Error(); err != nil {
		i.stderrLogger.Printf("error: %s\n", response.Error())
	}
}

func (i *Importer) queryExecutor(command string) {
	i.totalCommands++
	i.execute(command)
}

func (i *Importer) batchAccumulator(line string) {
	i.batch = append(i.batch, line)
	if len(i.batch) == i.batchSize {
		i.batchWrite()
	}
}

func (i *Importer) batchWrite() {
	// Exit early if there are no points in the batch.
	if len(i.batch) == 0 {
		return
	}

	// Accumulate the batch size to see how many points we have written this second
	i.throttlePointsWritten += len(i.batch)

	// Find out when we last wrote data
	since := time.Since(i.lastWrite)

	// Check to see if we've exceeded our points per second for the current timeframe
	var currentPPS int
	if since.Seconds() > 0 {
		currentPPS = int(float64(i.throttlePointsWritten) / since.Seconds())
	} else {
		currentPPS = i.throttlePointsWritten
	}

	// If our currentPPS is greater than the PPS specified, then we wait and retry
	if int(currentPPS) > i.config.PPS && i.config.PPS != 0 {
		// Wait for the next tick
		<-i.throttle.C

		// Decrement the batch size back out as it is going to get called again
		i.throttlePointsWritten -= len(i.batch)
		i.batchWrite()
		return
	}

	_, e := i.client.WriteLineProtocol(strings.Join(i.batch, "\n"), i.database, i.retentionPolicy, i.config.Precision, i.config.WriteConsistency)
	if e != nil {
		i.stderrLogger.Println("error writing batch: ", e)
		i.stderrLogger.Println(strings.Join(i.batch, "\n"))
		i.failedInserts += len(i.batch)
	} else {
		i.totalInserts += len(i.batch)
	}
	i.throttlePointsWritten = 0
	i.lastWrite = time.Now()

	// Clear the batch and record the number of processed points.
	i.batch = i.batch[:0]

	if i.progress != nil {
		i.progress(i.totalInserts, i.failedInserts)
	}
}
