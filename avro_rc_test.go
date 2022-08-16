package avro_rc

import (
	"github.com/qjacekk/avro_rc/gen_avro"
	"fmt"
	"os"
	"testing"
)
const sample1M = "./test_data/sample1M.avro"
const folder = "./test_data/folder"
const badFolder = "./test_data/bad"
func CheckTestData() {
	// tests require sample avro files:
	// ./test_data/sample1M.avro
	// and 20 copies of this file in ./test_data/folder dir
	_,err := os.Stat(sample1M)
	if err != nil {
		gen_avro.Gen(sample1M, 1000000)
	}
	if _,err := os.Stat(folder); os.IsNotExist(err) {
		os.MkdirAll(folder, os.ModePerm)
		for i:=0; i<20; i++ {
			gen_avro.Gen(fmt.Sprintf("%s/1M_%d.avro", folder, i), 1000000)
		}
	}
}
// Bad, malformed data, should be available in the repo
func CheckBadData() bool {
	if _,err := os.Stat(folder); os.IsNotExist(err) {
		return false
	}
	return true
}
func TestScanAvroFile(t *testing.T) {
	CheckTestData()
	if rc,err:= ScanAvroFile(sample1M, BuffSize); rc != 1000000 || err != nil {
		t.Errorf("Failed with row count: %d\n", rc)
	}
}
func TestScanPathSingle(t *testing.T) {
	CheckTestData()
	rc,s,fc := ScanPath(sample1M, "*.avro")
	if rc != 1000000 || s != 12745328 || fc != 1 {
		t.Errorf("Failed with rc: %d, s: %d, f: %d\n", rc,s,fc)
	}
}
func TestScanPathDir(t *testing.T) {
	CheckTestData()
	rc,s,fc := ScanPath(folder, "*.avro")
	if rc != 20000000 || s != 254906560 || fc != 20 {
		t.Errorf("Failed with rc: %d, s: %d, f: %d\n", rc,s,fc)
	}
}
func TestScanPathDirMT(t *testing.T) {
	CheckTestData()
	rc,s,fc := ScanPathMT(folder, "*.avro")
	if rc != 20000000 || s != 254906560 || fc != 20 {
		t.Errorf("Failed with rc: %d, s: %d, f: %d\n", rc,s,fc)
	}
}

func TestScanPathDirMTBad(t *testing.T) {
	if CheckBadData() {
		rc,s,fc := ScanPathMT(badFolder, "*.avro")
		// Expect 2 valid files each 1k records
		if rc != 2000 || s != 23168 || fc != 2 {
			t.Errorf("Failed with rc: %d, s: %d, f: %d\n", rc,s,fc)
		}
	} else {
		t.Skip("Missing test data in " + badFolder)
	}
}

func BenchmarkScanAvroFile(b *testing.B) {
	CheckTestData()
	fileName := "test_data/sample1M.avro"
	for i := 0; i < b.N; i++ {
		ScanAvroFile(fileName, BuffSize)
	}
}

func BenchmarkAvroScanPath(b *testing.B) {
	CheckTestData()
	fileName := "test_data/folder"
	for i := 0; i < b.N; i++ {
		ScanPath(fileName, "*.avro")
	}
}
func BenchmarkAvroScanPathMT(b *testing.B) {
	CheckTestData()
	fileName := "test_data/folder"
	for i := 0; i < b.N; i++ {
		ScanPathMT(fileName, "*.avro")
	}
}

/*
func BenchmarkScanAvroBuffSize(b *testing.B) {
	CheckTestData()
	fileName := "test_data/sample1M.avro"
	sizes :=             []string{"1k", "2k", "4k", "8k",   "16k",   "32k",   "64k",   "256k",   "384k",   "512k",   "1M",      "10M"}
	for i,buffSize := range []int{1024, 2048, 4096, 8*1024, 16*1024, 32*1024, 64*1024, 256*1024, 384*1024, 512*1024, 1024*1024, 10*1024*1024} {
		b.Run(fmt.Sprintf("ScanAvro-%d_buffSize=%s", i, sizes[i]),
		func(b *testing.B) {
			for j := 0; j < b.N; j++ {
				ScanAvro(fileName, buffSize)
			}
		})
	}
}
*/

/* benchamarking buffer size
to repeat the test: uncomment and run:
> go test -benchmem -bench="BenchmarkScanAvroBuffSize"

goos: windows
goarch: amd64
pkg: avro_rc
cpu: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
BenchmarkScanAvroBuffSize/ScanAvro-0_buffSize=1k-12         	      34	  33954597 ns/op	    2512 B/op	      15 allocs/op
BenchmarkScanAvroBuffSize/ScanAvro-1_buffSize=2k-12         	      66	  18096758 ns/op	    3536 B/op	      15 allocs/op
BenchmarkScanAvroBuffSize/ScanAvro-2_buffSize=4k-12         	     100	  10340143 ns/op	    5584 B/op	      15 allocs/op
BenchmarkScanAvroBuffSize/ScanAvro-3_buffSize=8k-12         	     190	   6206368 ns/op	    9680 B/op	      15 allocs/op
BenchmarkScanAvroBuffSize/ScanAvro-4_buffSize=16k-12        	     285	   4146818 ns/op	   17872 B/op	      15 allocs/op
BenchmarkScanAvroBuffSize/ScanAvro-5_buffSize=32k-12        	     382	   3109079 ns/op	   34256 B/op	      15 allocs/op <- 32k seems reasonable default
BenchmarkScanAvroBuffSize/ScanAvro-6_buffSize=64k-12        	     465	   2585380 ns/op	   67027 B/op	      15 allocs/op
BenchmarkScanAvroBuffSize/ScanAvro-7_buffSize=256k-12       	     517	   2354956 ns/op	  263636 B/op	      15 allocs/op
BenchmarkScanAvroBuffSize/ScanAvro-8_buffSize=384k-12       	     496	   2356089 ns/op	  394708 B/op	      15 allocs/op <- this is my CPU L1 cache size
BenchmarkScanAvroBuffSize/ScanAvro-9_buffSize=512k-12       	     508	   2343186 ns/op	  525777 B/op	      15 allocs/op
BenchmarkScanAvroBuffSize/ScanAvro-10_buffSize=1M-12        	     495	   2460082 ns/op	 1050066 B/op	      15 allocs/op
BenchmarkScanAvroBuffSize/ScanAvro-11_buffSize=10M-12       	     328	   3695112 ns/op	10487253 B/op	      15 allocs/op
PASS
ok  	avro_rc	17.430s
*/
