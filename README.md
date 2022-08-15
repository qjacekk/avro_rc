# avro_rc
Avro record counter

This is a simple command line tool to calcualte the number of records in Avro files.

It can process a single file or a directory of files (e.g. daily partition).

It returns total row count, total size and the number of files.

## Why
- I need a tool that would give me statistics for daily or hourly partition of Avro files that are going to be ingested into HDFS/Hive,
- existing tools are slow, either because they require JVM to start or because they decode every single record,
- calculating such stats in Hive can be extremely slow

## Acknowledgements
This tool uses a fast Go avro codec: https://github.com/hamba/avro

