// Package export exports TSM files into InfluxDB line protocol format.
package export

import (
	"archive/zip"
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/escape"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

type Progress func(progress float64)

// Exporter 根据 influx_inspect export 修改得来
type Exporter struct {
	dataDir         string
	walDir          string
	out             string
	database        string
	retentionPolicy string
	startTime       int64
	endTime         int64
	compress        bool
	lponly          bool
	totalRecords    int64
	completedFiles  int
	totalFiles      int
	progress        Progress
	manifest        map[string]struct{}
	tsmFiles        map[string][]string
	walFiles        map[string][]string
}

// NewExporter returns a new instance of Exporter.
func NewExporter(dataDir, walDir, database, outFile string) *Exporter {
	return &Exporter{
		dataDir:   dataDir,
		walDir:    walDir,
		out:       outFile,
		database:  database,
		startTime: math.MinInt64,
		endTime:   math.MaxInt64,
		manifest:  make(map[string]struct{}),
		tsmFiles:  make(map[string][]string),
		walFiles:  make(map[string][]string),
	}
}

func (e *Exporter) RetentionPolicy(retentionPolicy string) {
	e.retentionPolicy = retentionPolicy
}

func (e *Exporter) StartTime(startTime time.Time) {
	e.startTime = startTime.UnixNano()
}

func (e *Exporter) EndTime(endTime time.Time) {
	e.endTime = endTime.UnixNano()
}

func (e *Exporter) WithProgress(progress Progress) {
	e.progress = progress
}

func (e *Exporter) updateProgress() {
	if e.progress == nil {
		return
	}

	e.progress(float64(e.completedFiles) / float64(e.totalFiles))
}

// Export 开始导出数据
func (e *Exporter) Export() (int64, error) {
	if err := e.validate(); err != nil {
		return 0, err
	}

	return e.totalRecords, e.export()
}

func (e *Exporter) validate() error {
	if e.startTime >= e.endTime {
		return fmt.Errorf("end time before start time")
	}

	if e.database == "" {
		return fmt.Errorf("must specify a database")
	}
	return nil
}

func (e *Exporter) export() error {
	if err := e.walkTSMFiles(); err != nil {
		return err
	}
	if err := e.walkWALFiles(); err != nil {
		return err
	}

	return e.write()
}

func (e *Exporter) walkTSMFiles() error {
	return filepath.Walk(e.dataDir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// check to see if this is a tsm file
		if filepath.Ext(path) != "."+tsm1.TSMFileExtension {
			return nil
		}

		relPath, err := filepath.Rel(e.dataDir, path)
		if err != nil {
			return err
		}
		dirs := strings.Split(relPath, string(byte(os.PathSeparator)))
		if len(dirs) < 2 {
			return fmt.Errorf("invalid directory structure for %s", path)
		}
		if dirs[0] == e.database || e.database == "" {
			if dirs[1] == e.retentionPolicy || e.retentionPolicy == "" {
				key := filepath.Join(dirs[0], dirs[1])
				e.manifest[key] = struct{}{}
				e.tsmFiles[key] = append(e.tsmFiles[key], path)
			}
		}
		return nil
	})
}

func (e *Exporter) walkWALFiles() error {
	return filepath.Walk(e.walDir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// check to see if this is a wal file
		fileName := filepath.Base(path)
		if filepath.Ext(path) != "."+tsm1.WALFileExtension || !strings.HasPrefix(fileName, tsm1.WALFilePrefix) {
			return nil
		}

		relPath, err := filepath.Rel(e.walDir, path)
		if err != nil {
			return err
		}
		dirs := strings.Split(relPath, string(byte(os.PathSeparator)))
		if len(dirs) < 2 {
			return fmt.Errorf("invalid directory structure for %s", path)
		}
		if dirs[0] == e.database || e.database == "" {
			if dirs[1] == e.retentionPolicy || e.retentionPolicy == "" {
				key := filepath.Join(dirs[0], dirs[1])
				e.manifest[key] = struct{}{}
				e.walFiles[key] = append(e.walFiles[key], path)
			}
		}
		return nil
	})
}

//
//func (e *Exporter) writeDDL(mw io.Writer, w io.Writer) error {
//	// Write out all the DDL
//	fmt.Fprintln(mw, "# DDL")
//	for key := range e.manifest {
//		keys := strings.Split(key, string(os.PathSeparator))
//		db, rp := influxql.QuoteIdent(keys[0]), influxql.QuoteIdent(keys[1])
//		fmt.Fprintf(w, "CREATE DATABASE %s WITH NAME %s\n", db, rp)
//	}
//
//	return nil
//}

func (e *Exporter) writeDML(w io.Writer) error {
	for key := range e.manifest {
		if files, ok := e.tsmFiles[key]; ok {
			e.totalFiles += len(files)
		}
		if files, ok := e.walFiles[key]; ok {
			e.totalFiles += len(files)
		}
	}

	for key := range e.manifest {
		if files, ok := e.tsmFiles[key]; ok {
			if err := e.writeTsmFiles(w, files); err != nil {
				return err
			}
		}
		if _, ok := e.walFiles[key]; ok {
			if err := e.writeWALFiles(w, e.walFiles[key], key); err != nil {
				return err
			}
		}
	}

	return nil
}

// writeFull writes the full DML and DDL to the supplied io.Writers.  mw is the
// "meta" writer where comments and other informational writes go and w is for
// the actual payload of the writes -- DML and DDL.
//
// Typically mw and w are the same but if we'd like to, for example, filter out
// comments and other meta data, we can pass ioutil.Discard to mw to only
// include the raw data that writeFull() generates.
func (e *Exporter) writeFull(w io.Writer) error {

	if err := e.writeDML(w); err != nil {
		return err
	}

	return nil
}

func (e *Exporter) write() error {
	var w io.Writer
	f, err := os.Create(e.out)
	if err != nil {
		return err
	}
	defer f.Close()
	w = f

	bw := bufio.NewWriterSize(w, 1024*1024)
	defer bw.Flush()
	w = bw

	zw := zip.NewWriter(w)
	zdw, _ := zw.Create("influx.dat")

	defer func() {
		_ = zw.Close()
	}()
	w = zdw

	if err = e.writeFull(w); err != nil {
		return fmt.Errorf("备份数据失败, %+v", err)
	}

	meta := map[string]interface{}{
		"records": e.totalRecords,
		"dbType":  "influxdb1",
	}
	metaBytes, _ := json.Marshal(meta)

	if zmw, err := zw.Create("meta.json"); err != nil {
		return fmt.Errorf("创建元数据文件失败, %+v", err)
	} else {
		if _, err = zmw.Write(metaBytes); err != nil {
			return fmt.Errorf("生成元数据信息失败, %+v", err)
		}
	}

	return nil
}

func (e *Exporter) writeTsmFiles(w io.Writer, files []string) error {
	sort.Strings(files)

	for _, f := range files {
		if err := e.exportTSMFile(f, w); err != nil {
			return err
		} else {
			e.completedFiles += 1
			e.updateProgress()
		}
	}

	return nil
}

func (e *Exporter) exportTSMFile(tsmFilePath string, w io.Writer) error {
	f, err := os.Open(tsmFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(w, "skipped missing file: %s", tsmFilePath)
			return nil
		}
		return err
	}
	defer f.Close()

	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		fmt.Printf("unable to read %s, skipping: %s\r\n", tsmFilePath, err.Error())
		return nil
	}
	defer r.Close()

	if sgStart, sgEnd := r.TimeRange(); sgStart > e.endTime || sgEnd < e.startTime {
		return nil
	}

	for i := 0; i < r.KeyCount(); i++ {
		key, _ := r.KeyAt(i)
		values, err := r.ReadAll(key)
		if err != nil {
			fmt.Printf("unable to read key %q in %s, skipping: %s\r\n", string(key), tsmFilePath, err.Error())
			continue
		}
		measurement, field := tsm1.SeriesAndFieldFromCompositeKey(key)
		field = escape.Bytes(field)

		if err := e.writeValues(w, measurement, string(field), values); err != nil {
			// An error from writeValues indicates an IO error, which should be returned.
			return err
		}
	}
	return nil
}

func (e *Exporter) writeWALFiles(w io.Writer, files []string, key string) error {
	sort.Strings(files)

	var once sync.Once
	warnDelete := func() {
		once.Do(func() {
			msg := fmt.Sprintf(`WARNING: detected deletes in wal file.
Some series for %q may be brought back by replaying this data.
To resolve, you can either let the shard snapshot prior to exporting the data
or manually editing the exported file.
			`, key)
			fmt.Println(msg)
		})
	}

	for _, f := range files {
		if err := e.exportWALFile(f, w, warnDelete); err != nil {
			return err
		} else {
			e.completedFiles += 1
			e.updateProgress()
		}
	}

	return nil
}

// exportWAL reads every WAL entry from r and exports it to w.
func (e *Exporter) exportWALFile(walFilePath string, w io.Writer, warnDelete func()) error {
	f, err := os.Open(walFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(w, "skipped missing file: %s", walFilePath)
			return nil
		}
		return err
	}
	defer f.Close()

	r := tsm1.NewWALSegmentReader(f)
	defer r.Close()

	for r.Next() {
		entry, err := r.Read()
		if err != nil {
			n := r.Count()
			fmt.Printf("file %s corrupt at position %d: %v\r\n", walFilePath, n, err)
			break
		}

		switch t := entry.(type) {
		case *tsm1.DeleteWALEntry, *tsm1.DeleteRangeWALEntry:
			warnDelete()
			continue
		case *tsm1.WriteWALEntry:
			for key, values := range t.Values {
				measurement, field := tsm1.SeriesAndFieldFromCompositeKey([]byte(key))
				// measurements are stored escaped, field names are not
				field = escape.Bytes(field)

				if err := e.writeValues(w, measurement, string(field), values); err != nil {
					// An error from writeValues indicates an IO error, which should be returned.
					return err
				}
			}
		}
	}
	return nil
}

// writeValues writes every value in values to w, using the given series key and field name.
// If any call to w.Write fails, that error is returned.
func (e *Exporter) writeValues(w io.Writer, seriesKey []byte, field string, values []tsm1.Value) error {
	buf := []byte(string(seriesKey) + " " + field + "=")
	prefixLen := len(buf)

	for _, value := range values {
		ts := value.UnixNano()
		if (ts < e.startTime) || (ts > e.endTime) {
			continue
		}

		// Re-slice buf to be "<series_key> <field>=".
		buf = buf[:prefixLen]

		// Append the correct representation of the value.
		switch v := value.Value().(type) {
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
			buf = append(buf, 'i')
		case uint64:
			buf = strconv.AppendUint(buf, v, 10)
			buf = append(buf, 'u')
		case bool:
			buf = strconv.AppendBool(buf, v)
		case string:
			buf = append(buf, '"')
			buf = append(buf, models.EscapeStringField(v)...)
			buf = append(buf, '"')
		default:
			// This shouldn't be possible, but we'll format it anyway.
			buf = append(buf, fmt.Sprintf("%v", v)...)
		}

		// Now buf has "<series_key> <field>=<value>".
		// Append the timestamp and a newline, then write it.
		buf = append(buf, ' ')
		buf = strconv.AppendInt(buf, ts, 10)
		buf = append(buf, '\n')
		if _, err := w.Write(buf); err != nil {
			// Underlying IO error needs to be returned.
			return err
		}

		e.totalRecords++
	}

	return nil
}
