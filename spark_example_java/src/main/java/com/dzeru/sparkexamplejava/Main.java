package com.dzeru.sparkexamplejava;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;

public class Main {

    private static SparkSession spark = SparkSession
            .builder()
            .appName("spark_example_java")
            .master("local[*]")
            .getOrCreate();

    public static void main(String... args) {
        String path = args[0];
        HashMap<String, String> readOptions = new HashMap<>();
        readOptions.put("header", "true");
        readOptions.put("escape", "\"");
        Dataset<Row> aggSymbolsReadDF = spark.read().options(readOptions).csv(path);
        aggSymbolsReadDF.show(8);

        Dataset<Row> aggSymbolsCastDF = aggSymbolsReadDF.select(
                col("load_date").cast(TimestampType).as("loadDate"),
                col("symbol"),
                col("open_price").cast(createDecimalType(12, 4)).as("openPrice"),
                col("highest_price").cast(createDecimalType(12, 4)).as("highestPrice"),
                col("lowest_price").cast(createDecimalType(12, 4)).as("lowestPrice"),
                col("close_price").cast(createDecimalType(12, 4)).as("closePrice")
        );
        aggSymbolsCastDF.show(8);

        Dataset<Row> avgLowestPricesDF = aggSymbolsCastDF
                .select(col("loadDate"), col("symbol"), col("lowestPrice"))
                .groupBy(col("symbol"))
                .agg(avg("lowestPrice").as("avgLowestPrice"))
                .orderBy(desc("symbol"));
        avgLowestPricesDF.show();

        Dataset<Row> avgLowestPriceByDayDF = aggSymbolsCastDF
                .select(col("loadDate"), col("symbol"), col("lowestPrice"))
                .groupBy(col("symbol"), col("loadDate").cast(DateType).as("loadDate"))
                .agg(avg("lowestPrice").as("avgLowestPrice"))
                .orderBy(desc("symbol"));
        avgLowestPriceByDayDF.show();
    }
}
