package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"avro_rc"
)

// v0.1, Jacek Karas, 2022-08-11
//helper
func CheckErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
func main() {
	var pPattern = flag.String("p", "*.avro", "glob pattern to match file names if <path> is a directory")
	var st = flag.Bool("s", false, "use single thread only")
	var v = flag.Bool("v", false, "verbose")
	var bsize = flag.Int("b", avro_rc.BuffSize, "buffer size")
	flag.Usage = func () {
		fmt.Fprintln(flag.CommandLine.Output(), "Get Avro files statistics")
		fmt.Fprintln(flag.CommandLine.Output(), "  usage: avro_rc [options] <path>")
		fmt.Fprintln(flag.CommandLine.Output(), "     <path> a single file or directory to scan recursively.")
		fmt.Fprintln(flag.CommandLine.Output(), "  returns: row_count total_file_size num_of_files")
		fmt.Fprintln(flag.CommandLine.Output(), "Options:")
		flag.PrintDefaults()
	}
	flag.Parse()
	if _, err := filepath.Match(*pPattern, ""); err != nil {
		log.Fatalf("Invalid pattern: %s (%v)", *pPattern, err)
	}
	if avro_rc.BuffSize != *bsize {
		avro_rc.BuffSize = *bsize
	}
	if *v {
		avro_rc.Verbose = true
		log.Printf("Reader buffer size: %d\n", avro_rc.BuffSize)
		if *st {
			log.Println("Single thread mode")
		}
		log.Printf("Glob pattern: %s\n", *pPattern)
	}
	if flag.NArg() > 0 {
		var totalRowCnt, totalSize, fileCount int64
		if *st {
			totalRowCnt, totalSize, fileCount = avro_rc.ScanPath(flag.Arg(0), *pPattern)
		} else {
			totalRowCnt, totalSize, fileCount = avro_rc.ScanPathMT(flag.Arg(0),*pPattern)
		}
		fmt.Println(totalRowCnt, totalSize, fileCount)
	} else {
		flag.Usage()
	}
}
