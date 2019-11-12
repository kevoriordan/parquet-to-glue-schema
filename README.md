# parquet-to-glue-schema
Takes a parquet file and generate an AWS Glue table from it and load the partitions.

Usage:

`python3 generate.py <command line arguments>`

Command line arguments or omit command line arguments and type answers at prompt

`--s3-location`   : Location of your parquet files
`--database`  : Name of the glue database where you want to create the table
`--tablename` : The name you want to give the table in Glue.
`--results-bucket`  : Can be any S3 location. Athena will dump script results here after recreating partitions.
