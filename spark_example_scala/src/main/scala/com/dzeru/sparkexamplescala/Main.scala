package com.dzeru.sparkexamplescala

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Main {

  @transient lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("spark_example_scala")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  //val path: String = "/Users/dmashkina/spark_examples/agg_symbols.csv"

  def main(args: Array[String]): Unit = {
    val path = args(0)
    val aggSymbolsReadDF: DataFrame = spark.read.options(Map("header" -> "true", "escape" -> "\"")).csv(path)
    aggSymbolsReadDF.show(8)

    val aggSymbolsCastDF: DataFrame = aggSymbolsReadDF.select(
      $"load_date".cast(TimestampType).as("loadDate"),
      $"symbol",
      $"open_price".cast(DecimalType(12, 4)).as("openPrice"),
      $"highest_price".cast(DecimalType(12, 4)).as("highestPrice"),
      $"lowest_price".cast(DecimalType(12, 4)).as("lowestPrice"),
      $"close_price".cast(DecimalType(12, 4)).as("closePrice")
    )
    aggSymbolsCastDF.show(8)

    val avgLowestPricesDF: DataFrame = aggSymbolsCastDF
      .select($"loadDate", $"symbol", $"lowestPrice")
      .groupBy($"symbol")
      .agg(avg("lowestPrice").as("avgLowestPrice"))
      .orderBy(desc("symbol"))
    avgLowestPricesDF.show()

    val avgLowestPriceByDayDF: DataFrame = aggSymbolsCastDF
      .select($"loadDate", $"symbol", $"lowestPrice")
      .groupBy($"symbol", $"loadDate".cast(DateType).as("loadDate"))
      .agg(avg("lowestPrice").as("avgLowestPrice"))
      .orderBy(desc("symbol"))
    avgLowestPriceByDayDF.show()
  }
}