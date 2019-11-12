import pyarrow as pa
from pyarrow import types
import pyarrow.parquet as pq
import click
import jsons


def convertPyArrowTypeToGlueType(pyarrowType: pa.DataType) -> str:
    if (types.is_string(pyarrowType) or types.is_unicode(pyarrowType) or types.is_large_string(pyarrowType) or types.is_large_unicode(pyarrowType)):
        return 'string'
    if (types.is_int64(pyarrowType) or types.is_uint64(pyarrowType)):
        return 'bigint'
    if (types.is_binary(pyarrowType)):
        return 'binary'
    if (types.is_boolean(pyarrowType)):
        return 'boolean'
    if (types.is_date(pyarrowType) or types.is_date32(pyarrowType) or types.is_date64(pyarrowType)):
        return 'date'
    if (types.is_decimal(pyarrowType)):
        return 'decimal'
    if (types.is_float64(pyarrowType)):
        'return double'
    if (types.is_float16(pyarrowType) or types.is_float32(pyarrowType)):
        return 'float'
    if (types.is_int16(pyarrowType) or types.is_int32(pyarrowType) or types.is_uint16(pyarrowType) or types.is_uint32(pyarrowType)):
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
@click.option('--source', prompt='parquet file location', help='location of your parquet file(s)')
@click.option('--database', prompt='database name', help='Glue database/schema name')
@click.option('--tablename', prompt='table name', help='Table name in glue')
@click.option('--s3-location', prompt='s3 location', help='location of parquet files in s3')
@click.option('--partition-key', default='version', help='partition key')
def generate(source: str, database: str, tablename: str, s3_location: str, partition_key: str) -> None:
    schema = pq.read_schema(source)

    print(f'aws glue create-table --database-name {database} --table-input \'', end=' ')

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
        'PartitionKeys': [
            {
                'Name': partition_key,
                'Type': 'string'
            }
        ],
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'EXTERNAL': 'TRUE'
        }

    }

    print(jsons.dumps(table_input), end='\'')


if __name__ == "__main__":
    generate()
