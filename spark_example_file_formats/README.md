# Python

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

В примере используется формат данных Avro, который не поддерживается Spark по умолчанию. 
Для него требуется подключать отдельный пакет. 
Поэтому программу рекомендуется запускать в консоли следующим образом.

```
spark-submit --packages org.apache.spark:spark-avro_2.12:3.2.1 main.py FILEPATH/agg_symbols.csv
```

## Разбор кода

Импортируем модуль для работы со Spark. 
Также импортируем модуль ```sys``` для обращения к аргументам консоли.
Модуль ```os.path``` позволит работать с обычной файловой системой.

```python
import os
import sys
from os.path import getsize, join, dirname, realpath, abspath

from pyspark.sql import SparkSession
```

Создаем объект сессии Spark, который будет обрабатывать данные.

```python
spark = SparkSession \
    .builder \
```

Так будет называться приложение.

```python
.appName("spark_example_file_formats") \
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
    .appName("spark_example_file_formats") \
    .master("local[*]") \
    .getOrCreate()
```

Задаем путь к входному файлу. В данном случае путь передается как аргумент консоли.
Стоит помнить, что в Python нулевым аргументом консоли является сам скрипт, 
поэтому сами аргументы скрипта отсчитываются с первого.

```python
path = sys.argv[1]
```

Задаем пути к выходным файлам форматов ORC, Avro и Parquet.

```python
orc_path = '../output_files/agg_symbols_orc'
avro_path = '../output_files/agg_symbols.avro'
parquet_path = '../output_files/agg_symbols.parquet'
```

Задаем путь к файлу со результатом сравнения размеров файлов разных форматов.
Этот файл будет лежать в корневой папке программы.
```realpath(__file__)``` позволяет получить полный путь к файлу запускаемого скрипта.
```dirname``` вычленяет оттуда название папки, убирая название файла.
Этот путь объединяется с ```os.pardir```, что позволяет получить родительскую папку,
чтобы файл со статистикой не лежал в папке исходного кода ```src```.
Далее находится абсолютный путь к этой родительской папке с помощью ```abspath```.
В итоге полученный путь объединяется непосредственно с названием файла.

```python
stats_path = join(abspath(join(dirname(realpath(__file__)), os.pardir)), 'stats.txt')
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

Без всяких преобразований записываем считанные данные в формат ORC.

```python
aggSymbolsReadDF.write.orc(orc_path)
```

У Spark нет встроенного модуля для работы с форматом Avro, 
поэтому вызов записи производится по-другому.

```python
aggSymbolsReadDF.write.format('com.databricks.spark.avro').save(avro_path)
```

Сохраняем данные в формате Parquet.

```python
aggSymbolsReadDF.write.parquet(parquet_path)
```

Получаем размеры сохраненных файлов в байтах.

```python
csv_size = getsize(csv_path)
orc_size = getsize(orc_path)
avro_size = getsize(avro_path)
parquet_size = getsize(parquet_path)
```

Записываем полученную статистику в файл _stats.txt_.

```python
stats = f'CSV: {csv_size}\nORC: {orc_size}\nAvro: {avro_size}\nParquet: {parquet_size}\n\n' \
        f'CSV/ORC: {csv_size/orc_size}\n' \
        f'CSV/Avro: {csv_size/avro_size}\n' \
        f'CSV/Parquet: {csv_size/parquet_size}'

with open(stats_path) as res:
    res.write(stats)
```

Пример файла статистики.

```python
CSV: 416
ORC: 192
Avro: 192
Parquet: 192

CSV/ORC: 2.1666666666666665
CSV/Avro: 2.1666666666666665
CSV/Parquet: 2.1666666666666665
```