# Spark Example Query Plans

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

Перед началом работы необходимо установить PySpark.

```python
pip install pyspark
```

Для удобства обнаружения Spark на машине можно использовать библиотеку findspark.

```python
pip install findspark
```

До импорта модулей Spark необходимо импортировать эту библиотеку и вызвать метод ```init```.

```python
import findspark
findspark.init()
```

## Разбор кода

Импортируем нужные модули для работы с SQL, функциями и типами Spark. 
Также импортируем модуль ```sys``` для обращения к аргументам консоли.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
```

Создаем объект сессии Spark, который будет обрабатывать данные.

```python
spark = SparkSession \
    .builder \
```

Так будет называться приложение.

```python
.appName("spark_example_query_plans") \
```

Данная версия запускается локально.

```python
.master("local[*]") \
```

Если сессия существует, возьмется созданная версия, иначе создастся новая.

```python
.getOrCreate()
```

В итоге формируется следующий объект:

```python
spark = SparkSession \
    .builder \
    .appName("spark_example_query_plans") \
    .master("local[*]") \
    .getOrCreate()
```

Задаем путь к входному файлу. В данном случае путь передается как аргумент консоли.
Стоит помнить, что в Python нулевым аргументом консоли является сам скрипт, 
поэтому сами аргументы скрипта отсчитываются с первого.

```python
path = sys.argv[1]
```

Считываем файл в формате CSV по заданному пути.
Файл считывается в объект класса ```DataFrame```.
Учитываем, что у файла есть заголовок, а escape задан двойными кавычками.
Стоит отметить, что, в отличие от версий на Java и Scala, 
в Python в качестве опций чтения передается не словарь, а несколько вызовов отдельных опций.
Это представление похоже на реляционную таблицу, которая лежит в распределенной памяти кластера.

```python
aggSymbolsReadDF = spark.read.option("header", "true"). option("escape", "\"").csv(path)
```

Выводим в консоль 8 первых считанных строк.

```python
aggSymbolsReadDF.show(8)
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

```python
aggSymbolsCastDF = aggSymbolsReadDF.select(...)
```

Выбираем колонку ```load_date``` и преобразуем ее из строки в timestamp. 
После преобразования можем переименовать колонку с помощью функции ```alias```.
В отличие от Java и Scala, где для переименования используется ```as```,
в Python это слово является ключевым, используется в контекстном менеджере.
Поэтому для названия функции было выбрано ```alias```.

```python
col("load_date").cast(TimestampType()).alias("loadDate")
```

Также в запросе выбираем колонку ```open_price``` и преобразовываем ее в тип decimal, 
содержащий 12 символов всего и 4 в дробной части. Колонка также переименована.

```python
col("open_price").cast(DecimalType(12, 4)).alias("openPrice")
```

Аналогично преобразовываем остальные колонки. В итоге получается следующий селект-запрос:

```python
aggSymbolsCastDF = aggSymbolsReadDF.select(
        col("load_date").cast(TimestampType()).alias("loadDate"),
        col("symbol"),
        col("open_price").cast(DecimalType(12, 4)).alias("openPrice"),
        col("highest_price").cast(DecimalType(12, 4)).alias("highestPrice"),
        col("lowest_price").cast(DecimalType(12, 4)).alias("lowestPrice"),
        col("close_price").cast(DecimalType(12, 4)).alias("closePrice"))
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

```python
avgLowestPricesDF = aggSymbolsCastDF \
        .select(col("loadDate"), col("symbol"), col("lowestPrice")) \
        .groupBy(col("symbol")) \
        .agg(avg("lowestPrice").alias("avgLowestPrice")) \
        .orderBy(desc("symbol"))
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

Этот запрос достаточно простой, однако даже на нем будут видны различия в планах запроса.
План выполнения запроса можно изучить с помощью функции ```explain```.

Для начала вызовем эту функцию без каких-либо аргументов.

```python
avgLowestPricesExplain = aggSymbolsCastDF \
        .select(col("loadDate"), col("symbol"), col("lowestPrice")) \
        .groupBy(col("symbol")) \
        .agg(avg("lowestPrice").alias("avgLowestPrice")) \
        .orderBy(desc("symbol")).explain()
```

Вывод в консоль:

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [symbol#17 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(symbol#17 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [id=#97]
      +- HashAggregate(keys=[symbol#17], functions=[avg(lowestPrice#62)])
         +- Exchange hashpartitioning(symbol#17, 200), ENSURE_REQUIREMENTS, [id=#94]
            +- HashAggregate(keys=[symbol#17], functions=[partial_avg(lowestPrice#62)])
               +- Project [symbol#17, cast(lowest_price#20 as decimal(12,4)) AS lowestPrice#62]
                  +- FileScan csv [symbol#17,lowest_price#20] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/dmashkina/spark_examples/agg_symbols.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<symbol:string,lowest_price:string>
```

Как можно заметить, вывелся только физический план запроса.

Стоит отметить, что план запроса читается снизу вверх.
Например, по плану видно, что в последней строчке описано чтение данных из CSV-файла, 
которое происходит в самом начале выполнения запроса.
Далее строится проекция из двух столбцов: ```symbol``` и ```lowestPrice```.
Так как в запросе считается среднее наименьших цен, изначально происходит частичная агрегация,
что видно по ```partial_avg``` в строке с ```HashAggregate```.



```python
avgLowestPricesExplainTrue = aggSymbolsCastDF \
        .select(col("loadDate"), col("symbol"), col("lowestPrice")) \
        .groupBy(col("symbol")) \
        .agg(avg("lowestPrice").alias("avgLowestPrice")) \
        .orderBy(desc("symbol")).explain(True)
```



```python
avgLowestPricesExplainModeSimple = aggSymbolsCastDF \
        .select(col("loadDate"), col("symbol"), col("lowestPrice")) \
        .groupBy(col("symbol")) \
        .agg(avg("lowestPrice").alias("avgLowestPrice")) \
        .orderBy(desc("symbol")).explain(mode='simple')
```



```python
avgLowestPricesExplainModeExtended = aggSymbolsCastDF \
        .select(col("loadDate"), col("symbol"), col("lowestPrice")) \
        .groupBy(col("symbol")) \
        .agg(avg("lowestPrice").alias("avgLowestPrice")) \
        .orderBy(desc("symbol")).explain(mode='extended')
```


```python
avgLowestPricesExplainModeCodegen = aggSymbolsCastDF \
        .select(col("loadDate"), col("symbol"), col("lowestPrice")) \
        .groupBy(col("symbol")) \
        .agg(avg("lowestPrice").alias("avgLowestPrice")) \
        .orderBy(desc("symbol")).explain(mode='codegen')
```

```python
avgLowestPricesExplainModeCost = aggSymbolsCastDF \
        .select(col("loadDate"), col("symbol"), col("lowestPrice")) \
        .groupBy(col("symbol")) \
        .agg(avg("lowestPrice").alias("avgLowestPrice")) \
        .orderBy(desc("symbol")).explain(mode='cost')
```


```python
avgLowestPricesExplainModeFormatted = aggSymbolsCastDF \
        .select(col("loadDate"), col("symbol"), col("lowestPrice")) \
        .groupBy(col("symbol")) \
        .agg(avg("lowestPrice").alias("avgLowestPrice")) \
        .orderBy(desc("symbol")).explain(mode='formatted')
```