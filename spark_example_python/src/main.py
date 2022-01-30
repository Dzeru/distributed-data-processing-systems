import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

# pip install findspark
# pip install pyspark

spark = SparkSession \
    .builder \
    .appName("spark_example_python") \
    .master("local[*]") \
    .getOrCreate()

path = sys.argv[1]
aggSymbolsReadDF = spark.read.option("header", "true"). option("escape", "\"").csv(path)
aggSymbolsReadDF.show(8)

aggSymbolsCastDF = aggSymbolsReadDF.select(
        col("load_date").cast(TimestampType()).alias("loadDate"),
        col("symbol"),
        col("open_price").cast(DecimalType(12, 4)).alias("openPrice"),
        col("highest_price").cast(DecimalType(12, 4)).alias("highestPrice"),
        col("lowest_price").cast(DecimalType(12, 4)).alias("lowestPrice"),
        col("close_price").cast(DecimalType(12, 4)).alias("closePrice"))
aggSymbolsCastDF.show(8)

avgLowestPricesDF = aggSymbolsCastDF \
        .select(col("loadDate"), col("symbol"), col("lowestPrice")) \
        .groupBy(col("symbol")) \
        .agg(avg("lowestPrice").alias("avgLowestPrice")) \
        .orderBy(desc("symbol"))
avgLowestPricesDF.show()

avgLowestPriceByDayDF = aggSymbolsCastDF \
        .select(col("loadDate"), col("symbol"), col("lowestPrice")) \
        .groupBy(col("symbol"), col("loadDate").cast(DateType()).alias("loadDate")) \
        .agg(avg("lowestPrice").alias("avgLowestPrice")) \
        .orderBy(desc("symbol"))
avgLowestPriceByDayDF.show()
