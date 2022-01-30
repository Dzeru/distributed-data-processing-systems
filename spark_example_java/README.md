# Java

## Исходный файл

Данные для обработки содержатся в файле _agg_symbols.csv_.

```
load_date,symbol,open_price,highest_price,lowest_price,close_price
2021-12-10 13:00,A,154,154,154,154
2021-12-10 17:00,A,154.53,155.7,154.08,154.87
2021-12-10 21:00,A,154.9,156.34,154.72,156.23
2021-12-11 01:00,A,156.26,156.42,156.26,156.42
2021-12-10 13:00,AA,50.26,50.26,50.02,50.1
2021-12-10 17:00,AA,50.14,50.47,47.84,48.555
2021-12-10 21:00,AA,48.52,48.87,48.3799,48.78
2021-12-11 01:00,AA,48.8,48.9,48.46,48.76
```

* `load_date` - дата и время выгрузки данных;
* `symbol` - название акции;
* `open_price` - цена открытия периода;
* `highest_price` - наибольшая цена за период;
* `lowest_price` - наименьшая цена за период;
* `close_price` - цена закрытия периода.

## Перед началом работы

Через Maven необходимо подключить 2 зависимости: Spark Core и Spark SQL версии 3.2.0.
Стоит отметить, что в ```artifactId``` после названия версии артефакта
указывается требуемая версия языка Scala.

```xml
<dependencies>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_2.12</artifactId>
        <version>3.2.0</version>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_2.12</artifactId>
        <version>3.2.0</version>
    </dependency>
</dependencies>
```

## Разбор кода

В каком пакете лежит код.

```java
package com.dzeru.sparkexamplejava;
```

Импортируем нужные пакеты для работы с SQL, функциями и типами Spark. Также импортируем класс словаря ```HashMap```.

```java
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;
```

Объявляем класс, в котором будет описан код программы.

```java
public class Main
```

Создаем объект сессии Spark, который будет обрабатывать данные.

```java
private static SparkSession spark = SparkSession
        .builder()
```

Так будет называться приложение.

```java
.appName("spark_example_java")
```

Данная версия запускается локально.

```java
.master("local[*]")
```

Если сессия существует, возьмется созданная версия, иначе создастся новая.

```java
.getOrCreate();
```

В итоге формируется следующий объект:

```java
private static SparkSession spark = SparkSession
        .builder()
        .appName("spark_example_java")
        .master("local[*]")
        .getOrCreate();
```

Функция, в которой будет описана основная логика.

```java
public static void main(String... args)
```

Задаем путь к входному файлу. В данном случае путь передается как аргумент консоли.

```java
String path = args[0];
```

Создаем словарь с опциями чтения файла. 
Учитываем, что у файла есть заголовок, а escape задан двойными кавычками.

```java
HashMap<String, String> readOptions = new HashMap<>();
        readOptions.put("header", "true");
        readOptions.put("escape", "\"");
```

Считываем файл в формате CSV по заданному пути.
Файл считывается в объект класса ```Dataset<Row>```. 
В Java это аналог  ```DataFrame``` из Scala.
В качестве опций чтения передаем ранее созданный словарь.
Это представление похоже на реляционную таблицу, которая лежит в распределенной памяти кластера.

```java
Dataset<Row> aggSymbolsReadDF = spark.read().options(readOptions).csv(path);
```

Выводим в консоль 8 первых считанных строк.

```java
aggSymbolsReadDF.show(8);
```

Вывод в консоль:

```
+----------------+------+----------+-------------+------------+-----------+
|       load_date|symbol|open_price|highest_price|lowest_price|close_price|
+----------------+------+----------+-------------+------------+-----------+
|2021-12-10 13:00|     A|       154|          154|         154|        154|
|2021-12-10 17:00|     A|    154.53|        155.7|      154.08|     154.87|
|2021-12-10 21:00|     A|     154.9|       156.34|      154.72|     156.23|
|2021-12-11 01:00|     A|    156.26|       156.42|      156.26|     156.42|
|2021-12-10 13:00|    AA|     50.26|        50.26|       50.02|       50.1|
|2021-12-10 17:00|    AA|     50.14|        50.47|       47.84|     48.555|
|2021-12-10 21:00|    AA|     48.52|        48.87|     48.3799|      48.78|
|2021-12-11 01:00|    AA|      48.8|         48.9|       48.46|      48.76|
+----------------+------+----------+-------------+------------+-----------+
```

Редактируем считанный датасет. Для этого необходимо сделать селект-запрос к его полям.

```java
Dataset<Row> aggSymbolsCastDF = aggSymbolsReadDF.select(...);
```

Выбираем колонку ```load_date``` и преобразуем ее из строки в timestamp. 
После преобразования можем переименовать колонку с помощью функции ```as```.

```java
col("load_date").cast(TimestampType).as("loadDate")
```

Также в запросе выбираем колонку ```open_price``` и преобразовываем ее в тип decimal, 
содержащий 12 символов всего и 4 в дробной части. Колонка также переименована.

```java
col("open_price").cast(createDecimalType(12, 4)).as("openPrice")
```

Аналогично преобразовываем остальные колонки. В итоге получается следующий селект-запрос:

```java
Dataset<Row> aggSymbolsCastDF = aggSymbolsReadDF.select(
        col("load_date").cast(TimestampType).as("loadDate"),
        col("symbol"),
        col("open_price").cast(createDecimalType(12, 4)).as("openPrice"),
        col("highest_price").cast(createDecimalType(12, 4)).as("highestPrice"),
        col("lowest_price").cast(createDecimalType(12, 4)).as("lowestPrice"),
        col("close_price").cast(createDecimalType(12, 4)).as("closePrice")
        );
```

Вывод в консоль:

```
+-------------------+------+---------+------------+-----------+----------+
|           loadDate|symbol|openPrice|highestPrice|lowestPrice|closePrice|
+-------------------+------+---------+------------+-----------+----------+
|2021-12-10 13:00:00|     A| 154.0000|    154.0000|   154.0000|  154.0000|
|2021-12-10 17:00:00|     A| 154.5300|    155.7000|   154.0800|  154.8700|
|2021-12-10 21:00:00|     A| 154.9000|    156.3400|   154.7200|  156.2300|
|2021-12-11 01:00:00|     A| 156.2600|    156.4200|   156.2600|  156.4200|
|2021-12-10 13:00:00|    AA|  50.2600|     50.2600|    50.0200|   50.1000|
|2021-12-10 17:00:00|    AA|  50.1400|     50.4700|    47.8400|   48.5550|
|2021-12-10 21:00:00|    AA|  48.5200|     48.8700|    48.3799|   48.7800|
|2021-12-11 01:00:00|    AA|  48.8000|     48.9000|    48.4600|   48.7600|
+-------------------+------+---------+------------+-----------+----------+
```

Далее будет вычислено среднее значение наименьших цен относительно акций ```symbol``` и даты ```loadDate```.

Будет создан ```DataFrame``` из селект-запроса.
Выбирается только 3 поля: ```loadDate```, ```symbol``` и ```lowestPrice```.
Данные группируются по названию акций.
Далее вызывается функция ```agg```, которая принимает одну из конкретных функций агрегации, 
в данном случае это функция поиска среднего значения ```avg``` по колонке ```lowestPrice```.
Для удобства тоже переименуем эту колонку.
Данные будут выводиться в убывающем порядке относительно колонки ```symbol```.

Стоит отметить, что, несмотря на 3 выбранных поля, в результате выведется только 2, 
потому что поле ```loadDate``` не встречается ни в функции группировки, ни в функции агрегации.

```java
Dataset<Row> avgLowestPricesDF = aggSymbolsCastDF
        .select(col("loadDate"), col("symbol"), col("lowestPrice"))
        .groupBy(col("symbol"))
        .agg(avg("lowestPrice").as("avgLowestPrice"))
        .orderBy(desc("symbol"));
```

Вывод в консоль:

```
+------+--------------+
|symbol|avgLowestPrice|
+------+--------------+
|    AA|   48.67497500|
|     A|  154.76500000|
+------+--------------+
```

Чтобы выводились все 3 поля, добавим в функцию группировки колонку ```loadDate```.
Однако в исходных данных эта колонка содержит уникальные и дату, и время, что сводит на нет смысл группировки.
Для этого сконвертируем ее из timestamp в date, чтобы оставить только дату. 
В остальном запрос аналогичен предыдущему.

```java
Dataset<Row> avgLowestPriceByDayDF = aggSymbolsCastDF
        .select(col("loadDate"), col("symbol"), col("lowestPrice"))
        .groupBy(col("symbol"), col("loadDate").cast(DateType).as("loadDate"))
        .agg(avg("lowestPrice").as("avgLowestPrice"))
        .orderBy(desc("symbol"));
```

Вывод в консоль:

```
+------+----------+--------------+
|symbol|  loadDate|avgLowestPrice|
+------+----------+--------------+
|    AA|2021-12-11|   48.46000000|
|    AA|2021-12-10|   48.74663333|
|     A|2021-12-10|  154.26666667|
|     A|2021-12-11|  156.26000000|
+------+----------+--------------+
```