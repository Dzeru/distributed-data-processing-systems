import os
import sys
from os.path import getsize, join, dirname, realpath, abspath

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName('spark_example_file_formats') \
    .master('local[*]') \
    .getOrCreate()

csv_path = sys.argv[1]
orc_path = '../output_files/agg_symbols_orc'
avro_path = '../output_files/agg_symbols.avro'
parquet_path = '../output_files/agg_symbols.parquet'

stats_path = join(abspath(join(dirname(realpath(__file__)), os.pardir)), 'stats.txt')

# read CSV file
aggSymbolsReadDF = spark.read.option('header', 'true').option('escape', '\"').csv(csv_path)

# write ORC file
aggSymbolsReadDF.write.orc(orc_path)

# write Avro file
aggSymbolsReadDF.write.format('com.databricks.spark.avro').save(avro_path)

# write Parquet file
aggSymbolsReadDF.write.parquet(parquet_path)

csv_size = getsize(csv_path)
orc_size = getsize(orc_path)
avro_size = getsize(avro_path)
parquet_size = getsize(parquet_path)

stats = f'CSV: {csv_size}\nORC: {orc_size}\nAvro: {avro_size}\nParquet: {parquet_size}\n\n' \
        f'CSV/ORC: {csv_size/orc_size}\n' \
        f'CSV/Avro: {csv_size/avro_size}\n' \
        f'CSV/Parquet: {csv_size/parquet_size}'

with open(stats_path, 'w') as res:
    res.write(stats)
