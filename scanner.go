package avro_rc

import (
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"github.com/hamba/avro"
	"github.com/hamba/avro/ocf"
)

// v0.2, Jacek Karas, 2022-08-15
//helper
func CheckErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
var Verbose = false
// AVRO stuff
var BuffSize = 32*1024  // default reader buffer size, optimal value is around the size of CPU L1 cache
var magicBytes = [4]byte{'O', 'b', 'j', 1}
/* Old version with reading into memory, a bit slower than the one with SkipNBytes()
// Scans avro file to get the number of rows in the file.
func ScanAvroInt(fileName string, buffSize int) (int64, error) {
	f, err := os.Open(fileName)
	CheckErr(err)
	defer f.Close()

	reader := avro.NewReader(f, buffSize)
	var h ocf.Header
	reader.ReadVal(ocf.HeaderSchema, &h)
	if reader.Error != nil {
		return 0, fmt.Errorf("decoder: unexpected error: %w", reader.Error)
	}
	if h.Magic != magicBytes {
		return 0, fmt.Errorf("decoder: invalid avro file magic")
	}
	var rowCount int64
	var sync [16]byte
	var dsize int64 = 4096
	data := make([]byte, dsize)

	for reader.Error == nil {
		count := reader.ReadLong()
		size := reader.ReadLong()
		if count == 0 {
			break
		}
		rowCount += count
		if size > dsize {
			dsize = size
			data = make([]byte, dsize)
		}
		reader.Read(data[0:size])
		reader.Read(sync[:])
		if h.Sync != sync {
			fmt.Println("Invalid sync", sync)
		}	
	}
	return rowCount, nil
}
*/
func ScanAvro(r io.Reader, readerBuffSize int) (int64, error) {
	reader := avro.NewReader(r, readerBuffSize)
	var h ocf.Header
	reader.ReadVal(ocf.HeaderSchema, &h)
	if reader.Error != nil {
		return 0, fmt.Errorf("decoder: unexpected error: %w", reader.Error)
	}
	if h.Magic != magicBytes {
		return 0, fmt.Errorf("decoder: invalid avro file magic")
	}
	var rowCount int64
	var sync [16]byte

	for reader.Error == nil {
		count := reader.ReadLong()
		size := reader.ReadLong()
		if count == 0 {
			break
		}
		rowCount += count
		reader.SkipNBytes(int(size))
		reader.Read(sync[:])
		if h.Sync != sync {
			return 0, fmt.Errorf("decoder: invalid avro block sync")
		}
	}
	return rowCount, nil
}

func ScanAvroFile(fileName string, readerBuffSize int) (int64, error) {
	f, err := os.Open(fileName)
	CheckErr(err)
	defer f.Close()
	if Verbose {
		log.Printf("Scanning: %s\n", fileName)
	}
	rc,err:= ScanAvro(f, readerBuffSize)
	if Verbose {
		log.Printf("Finished scanning: %s\n", fileName)
	}
	return rc,err
}

// Scans file or directory
// Single-thread sequential version
func ScanPath(path string, pPattern string) (int64, int64, int64) {
	fi, err := os.Stat(path)
	CheckErr(err)
	if fi.Mode().IsRegular() {
		rc, err := ScanAvroFile(path, BuffSize)
		CheckErr(err)
		return rc, fi.Size(), 1
	} else if fi.IsDir() {
		var totalRowCnt int64
		var totalSize int64
		var fileCount int64
		filepath.Walk(path, func(path string, info fs.FileInfo, err error) error {
			CheckErr(err)
			if info.Mode().IsRegular() {
				if match, _ := filepath.Match(pPattern, info.Name()); match {
					rc, err := ScanAvroFile(path, BuffSize)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Unable to read file: %s, error: %s\n", path, err.Error())
						return nil
					}
					totalRowCnt += rc
					totalSize += info.Size()
					fileCount++
				}
			}
			return nil
		})
		return totalRowCnt, totalSize, fileCount
	}
	return 0,0,0
}


// Concurrent version
var wg sync.WaitGroup // waits for all workers to complete
var totalRowCnt int64
var totalSize int64
var fileCount int64

func worker(files <-chan *AvroFile) {
	for file := range files {
		rc, err := ScanAvroFile(file.path, BuffSize)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to read file: %s, error: %s\n", file.path, err.Error())
		} else {
			atomic.AddInt64(&totalRowCnt,rc)
			atomic.AddInt64(&totalSize, file.size)
			atomic.AddInt64(&fileCount, 1)
		}
	}
	wg.Done()
}
type AvroFile struct {
	path string
	size int64
}
var files chan *AvroFile
// Scans file or directory using multiple threads
func ScanPathMT(path string, pPattern string) (int64, int64, int64) {
	// init counters - this is necessary mainly for unit tests
	totalRowCnt = 0
	totalSize = 0
	fileCount = 0
	fi, err := os.Stat(path)
	CheckErr(err)
	if fi.Mode().IsRegular() {
		rc, err := ScanAvroFile(path, BuffSize)
		CheckErr(err)
		return rc, fi.Size(), 1
	} else if !fi.IsDir() {
		log.Fatal("input must be a file or directory")
	}
	files = make(chan *AvroFile)
	nWorkers := runtime.NumCPU()
	if Verbose {
		log.Printf("Starting %d workers\n", nWorkers)
	}
	for w := 1; w <= nWorkers; w++ {
        wg.Add(1)
        go worker(files)
    }
	filepath.Walk(path, func(path string, info fs.FileInfo, err error) error {
		CheckErr(err)
		if info.Mode().IsRegular() {
			if match, _ := filepath.Match(pPattern, info.Name()); match {
				files <- &AvroFile{path:path, size:info.Size()}
			}
		}
		return nil
	})
	close(files)
	wg.Wait()
	return totalRowCnt, totalSize, fileCount
}