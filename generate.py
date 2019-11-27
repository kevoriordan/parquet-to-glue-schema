import pyarrow as pa
from pyarrow import types
import pyarrow.parquet as pq
import click
import s3fs
import boto3


def convertPyArrowTypeToGlueType(pyarrowType: pa.DataType) -> str:
    if (types.is_string(pyarrowType) or types.is_unicode(pyarrowType)
            or types.is_large_string(pyarrowType) or types.is_large_unicode(pyarrowType)):
        return 'string'
    if (types.is_int64(pyarrowType) or types.is_uint64(pyarrowType)):
        return 'bigint'
    if (types.is_binary(pyarrowType)):
        return 'binary'
    if (types.is_boolean(pyarrowType)):
        return 'boolean'
    if (types.is_date(pyarrowType) or types.is_date32(
            pyarrowType) or types.is_date64(pyarrowType)):
        return 'date'
    if (types.is_decimal(pyarrowType)):
        return 'decimal(16,2)'
    if (types.is_float64(pyarrowType)):
        'return double'
    if (types.is_float16(pyarrowType) or types.is_float32(pyarrowType)):
        return 'float'
    if (types.is_int16(pyarrowType) or types.is_int32(pyarrowType)
            or types.is_uint16(pyarrowType) or types.is_uint32(pyarrowType)):
        return 'int'
    if (types.is_map(pyarrowType)):
        return 'map'
    if (types.is_struct(pyarrowType)):
        return 'struct'
    if (types.is_timestamp(pyarrowType)):
        return 'timestamp'
    if (types.is_union(pyarrowType)):
        return 'union'
    return str(pyarrowType)


@click.command()
@click.option('--s3-location', prompt='s3 location',
              help='location of your parquet file(s)')
@click.option('--database', prompt='database name',
              help='Glue database/schema name')
@click.option('--tablename', prompt='table name', help='Table name in glue')
@click.option('--results-bucket', prompt='results bucket')
def generate(s3_location: str, database: str, tablename: str,
             results_bucket: str) -> None:
    bucket = s3_location.split('/')[2]
    curr_prefix = '/'.join(s3_location.split('/')[3:])
    if not curr_prefix.endswith('/'):
        curr_prefix = curr_prefix + '/'
    s3 = boto3.client('s3')
    objects = s3.list_objects_v2(
        Bucket=bucket,
        Delimiter='/',
        Prefix=curr_prefix)
    partitions = []

    while 'CommonPrefixes' in objects.keys():
        common_prefixes = objects['CommonPrefixes'][0]['Prefix'].split('/')
        first_partition = common_prefixes[len(common_prefixes) - 2]
        print(first_partition)
        partition_key = first_partition.split('=')[0]
        partitions = partitions + [{
            'Name': partition_key,
            'Type': 'string'
        }]
        curr_prefix = curr_prefix + first_partition
        if not curr_prefix.endswith('/'):
            curr_prefix = curr_prefix + '/'
        objects = s3.list_objects_v2(
            Bucket=bucket, Delimiter='/', Prefix=curr_prefix)

    print(curr_prefix)
    objects = s3.list_objects_v2(
        Bucket=bucket,
        Delimiter='/',
        Prefix=curr_prefix + 'part')

    s3_file = 's3://' + bucket + '/' + objects['Contents'][0]['Key']

    fs = s3fs.S3FileSystem()
    dataset = pq.ParquetDataset(s3_file, filesystem=fs)
    schema = dataset.schema.to_arrow_schema()

    columns = []
    index = 0
    while (index < len(schema.names)):
        name = schema.names[index]
        dType = schema.types[index]
        columns = columns + [{
            'Name': name,
            'Type': convertPyArrowTypeToGlueType(dType)
        }]
        index = index + 1

    table_input = {
        "Name": tablename,
        'StorageDescriptor': {
            'Columns': columns,
            'Location': s3_location,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'Compressed': False,
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                'Parameters': {
                    'serialization.format': '1'
                }
            },
        },
        'PartitionKeys': partitions,
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'EXTERNAL': 'TRUE'
        }

    }

    glue = boto3.client('glue')
    glue.create_table(
        DatabaseName=database,
        TableInput=table_input
    )

    print("Created glue table")
    athena = boto3.client('athena')
    athena.start_query_execution(
        QueryString=f'MSCK REPAIR TABLE {tablename}',
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': results_bucket
        }
    )


if __name__ == "__main__":
    generate()
