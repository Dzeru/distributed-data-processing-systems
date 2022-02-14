import findspark
findspark.init()

from pprint import pprint

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd

# pip install findspark
# pip install pyspark

spark = SparkSession \
    .builder \
    .appName("spark_example_coalesce_repartition") \
    .master("local[*]") \
    .getOrCreate()

inputList = pd.DataFrame({'number': list(range(1, 11))})

inputListDF = spark.createDataFrame(inputList)
print('inputListDF partitions count:', inputListDF.rdd.getNumPartitions())
pprint(inputListDF.rdd.glom().collect())
print('\n')

coalesce2DF = inputListDF.coalesce(2)
print('coalesce2DF partitions count:', coalesce2DF.rdd.getNumPartitions())
pprint(coalesce2DF.rdd.glom().collect())
print('\n')

repartition2DF = inputListDF.repartition(2)
print('repartition2DF partitions count:', repartition2DF.rdd.getNumPartitions())
pprint(repartition2DF.rdd.glom().collect())
print('\n')

coalesce3DF = inputListDF.coalesce(3)
print('coalesce3DF partitions count:', coalesce3DF.rdd.getNumPartitions())
pprint(coalesce3DF.rdd.glom().collect())
print('\n')

repartition3DF = inputListDF.repartition(3)
print('repartition3DF partitions count:', repartition3DF.rdd.getNumPartitions())
pprint(repartition3DF.rdd.glom().collect())
print('\n')

repartitionColDF = inputListDF.repartition(col('number'))
print('repartitionColDF partitions count:', repartitionColDF.rdd.getNumPartitions())
pprint(repartitionColDF.rdd.glom().collect())
