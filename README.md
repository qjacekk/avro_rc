# avro_rc
Avro record counter

This is a simple command line tool to calcualte the number of records in Avro files.

It can process a single file or a directory of files (e.g. daily partition).

It returns total row count, total size and the number of files.

## Why
- I need a tool that would give me statistics for daily or hourly partition of Avro files that are going to be ingested into HDFS/Hive,
- existing tools are slow, either because they require JVM to start or because they decode every single record,
- calculating such stats in Hive can be extremely slow

## Build & usage

```commandline
$ cd cmd/avrc

$ go build

$ avrc -h

Get Avro files statistics
  usage: avro_rc [options] <path>
     <path> a single file or directory to scan recursively.
  returns: row_count total_file_size num_of_files
Options:
  -b int
        buffer size (default 32768)
  -p string
        glob pattern to match file names if <path> is a directory (default "*.avro")
  -s    use single thread only
  -v    verbose
```

> The reader buffer size can have significant impact on the speed. By default it's 32kb. Setting it to a value close to L1 cache of your CPU can give some additional gains.

> Scanning a directory is concurrent by default with the number of parallel workers equal to the number of logical CPUs. Use -s flag to scan files sequentially in a single thread (in case maxing all CPUs is not recommeded).

## Acknowledgements
This tool uses a fast Go avro codec: https://github.com/hamba/avro

