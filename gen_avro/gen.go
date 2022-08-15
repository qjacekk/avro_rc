package gen_avro

import (
	"fmt"
	"log"
	"os"
	"github.com/hamba/avro/ocf"
)

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func Gen(fileName string, nRecords int) {
	schema := `
	{
	    "type": "record",
	    "name": "test_file",
	    "namespace": "com.example",
	    "fields" : [
	        {"name": "f_string", "type": "string"},
	        {"name": "f_int", "type": "int"},
			{"name": "f_long", "type": "long"},
			{"name": "f_float", "type": "float"}
	    ]
	}
	`
	type Record struct {
		F_string string  `avro:"f_string"`
		F_int    int     `avro:"f_int"`
		F_long   int64   `avro:"f_long"`
		F_float  float32 `avro:"f_float"`
	}

	f, err := os.Create(fileName)
	checkErr(err)
	defer f.Close()

	enc, err := ocf.NewEncoder(schema, f, ocf.WithCodec(ocf.Snappy))
	checkErr(err)
	for i := 0; i < nRecords; i++ {
		var record = Record{F_string: fmt.Sprintf("string_%d", i), F_int: i, F_long: int64(i), F_float: float32(i)}
		enc.Encode(record)
	}
	err = enc.Flush()
	checkErr(err)
	err = f.Sync()
	checkErr(err)
}
