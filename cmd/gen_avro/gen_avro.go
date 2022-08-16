package main

import (
	"fmt"
	"log"
	"github.com/qjacekk/avro_rc/gen_avro"
)

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}


func main() {
	const nRecords = 1000
	fileName := "sample1k.avro"
	gen_avro.Gen(fileName, nRecords)
	fmt.Println("Generated", fileName)
}
