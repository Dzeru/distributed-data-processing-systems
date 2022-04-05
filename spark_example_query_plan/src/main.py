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
    .appName("spark_example_query_plans") \
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

avgLowestPricesDF = aggSymbolsCastDF \
        .select(col("loadDate"), col("symbol"), col("lowestPrice")) \
        .groupBy(col("symbol")) \
        .agg(avg("lowestPrice").alias("avgLowestPrice")) \
        .orderBy(desc("symbol"))
avgLowestPricesDF.show(8)

avgLowestPricesExplain = aggSymbolsCastDF \
        .select(col("loadDate"), col("symbol"), col("lowestPrice")) \
        .groupBy(col("symbol")) \
        .agg(avg("lowestPrice").alias("avgLowestPrice")) \
        .orderBy(desc("symbol")).explain()

avgLowestPricesExplainTrue = aggSymbolsCastDF \
        .select(col("loadDate"), col("symbol"), col("lowestPrice")) \
        .groupBy(col("symbol")) \
        .agg(avg("lowestPrice").alias("avgLowestPrice")) \
        .orderBy(desc("symbol")).explain(True)

avgLowestPricesExplainModeSimple = aggSymbolsCastDF \
        .select(col("loadDate"), col("symbol"), col("lowestPrice")) \
        .groupBy(col("symbol")) \
        .agg(avg("lowestPrice").alias("avgLowestPrice")) \
        .orderBy(desc("symbol")).explain(mode='simple')

avgLowestPricesExplainModeExtended = aggSymbolsCastDF \
        .select(col("loadDate"), col("symbol"), col("lowestPrice")) \
        .groupBy(col("symbol")) \
        .agg(avg("lowestPrice").alias("avgLowestPrice")) \
        .orderBy(desc("symbol")).explain(mode='extended')

avgLowestPricesExplainModeCodegen = aggSymbolsCastDF \
        .select(col("loadDate"), col("symbol"), col("lowestPrice")) \
        .groupBy(col("symbol")) \
        .agg(avg("lowestPrice").alias("avgLowestPrice")) \
        .orderBy(desc("symbol")).explain(mode='codegen')

avgLowestPricesExplainModeCost = aggSymbolsCastDF \
        .select(col("loadDate"), col("symbol"), col("lowestPrice")) \
        .groupBy(col("symbol")) \
        .agg(avg("lowestPrice").alias("avgLowestPrice")) \
        .orderBy(desc("symbol")).explain(mode='cost')

avgLowestPricesExplainModeFormatted = aggSymbolsCastDF \
        .select(col("loadDate"), col("symbol"), col("lowestPrice")) \
        .groupBy(col("symbol")) \
        .agg(avg("lowestPrice").alias("avgLowestPrice")) \
        .orderBy(desc("symbol")).explain(mode='formatted')
