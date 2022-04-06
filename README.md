# Курс распределенных систем обработки данных 

В этом репозитории лежат примеры по курсу распределенных систем обработки данных 
для 4 курса КНиИТ 2022 года.

## Список примеров

* __spark_example_java__ - пример простейшего приложения Spark на языке Java.
* __spark_example_python__ - пример простейшего приложения Spark на языке Python.
* __spark_example_scala__ - пример простейшего приложения Spark на языке Scala. 
* __spark_example_file_formats__ - сравнение выходных форматов данных при работе Spark.
* __spark_example_coalesce_repartition__ - сравнение работы coalesce и repartition в Spark.

## Исходный файл

Данные для примеров 
__spark_example_java__, __spark_example_python__, __spark_example_scala__ и __spark_example_file_formats__ 
содержатся в файле _agg_symbols.csv_.

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
