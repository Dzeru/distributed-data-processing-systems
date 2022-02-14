# Spark Example Coalesce and Repartition

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

Импортируем нужный модуль для работы со Spark SQL и библиотеку Pandas. 
Также импортируем модуль ```pprint``` для красивого вывода данных.

```python
from pprint import pprint

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
```

Создаем объект сессии Spark, который будет обрабатывать данные.

```python
spark = SparkSession \
    .builder \
```

Так будет называться приложение.

```python
.appName("spark_example_coalesce_repartition") \
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
    .appName("spark_example_coalesce_repartition") \
    .master("local[*]") \
    .getOrCreate()
```

В качестве входного набора данных берется список чисел от 1 до 10, 
преобразованный в Pandas DataFrame для совместимости со Spark.

```python
inputList = pd.DataFrame({'number': list(range(1, 11))})
```

Spark DataFrame создается из Pandas DataFrame.

```python
inputListDF = spark.createDataFrame(inputList)
```

Выводим в консоль количество партиций, созданных по умолчанию.

```python
print('inputListDF partitions count:', inputListDF.rdd.getNumPartitions())
```

С помощью ```pprint``` выводим данные, разделенные по партициям.

```python
pprint(inputListDF.rdd.glom().collect())
```

Вывод в консоль:

```
inputListDF partitions count: 8
[[Row(number=1)],
 [Row(number=2)],
 [Row(number=3)],
 [Row(number=4), Row(number=5)],
 [Row(number=6)],
 [Row(number=7)],
 [Row(number=8)],
 [Row(number=9), Row(number=10)]]
```

По умолчанию создалось восемь партиций. В шести из них всего по одному элементу, в двух — по два.

Это можно изменить с помощью двух функций Spark. 
Coalesce просто объединит несколько партиций в одну, чтобы уменьшить их итоговое количество.
Порядок данных не изменится.
Repartition перемешает данные полностью и заново пересчитает для них партиции
исходя из заданного числа партиций или колонки, по которой будет производиться разделение.

Для начала попробуем задать 2 партиции с помощью функции ```coalesce```.

```python
coalesce2DF = inputListDF.coalesce(2)
```

Аналогично выведем информацию о количестве и составе партиций.

```python
print('coalesce2DF partitions count:', coalesce2DF.rdd.getNumPartitions())
pprint(coalesce2DF.rdd.glom().collect())
```

Вывод в консоль:

```
coalesce2DF partitions count: 2
[[Row(number=1), Row(number=2), Row(number=3), Row(number=4), Row(number=5)],
 [Row(number=6), Row(number=7), Row(number=8), Row(number=9), Row(number=10)]]
```

Заметно, что количество партиций сократилось до двух, в каждой одинаковое количество элементов. 
Данные все еще упорядочены от 1 до 10.

Теперь попробуем разделить данные с помощью функции ```repartition```.

```python
repartition2DF = inputListDF.repartition(2)
```

Аналогично выведем информацию о количестве и составе партиций.

```python
print('repartition2DF partitions count:', repartition2DF.rdd.getNumPartitions())
pprint(repartition2DF.rdd.glom().collect())
```

Вывод в консоль:

```
repartition2DF partitions count: 2
[[Row(number=1),
  Row(number=2),
  Row(number=3),
  Row(number=5),
  Row(number=6),
  Row(number=7),
  Row(number=8),
  Row(number=10)],
 [Row(number=4), Row(number=9)]]
```

Партиций также две, но теперь они не сбалансированы: в одной восемь элементов, в другой два.
К тому же элементы теперь не упорядочены. Например, в одной партиции содержатся 3 и 5, в другой — 4.

Для сравнения вызовем эти функции для трех партиций.

```python
coalesce3DF = inputListDF.coalesce(3)
print('coalesce3DF partitions count:', coalesce3DF.rdd.getNumPartitions())
pprint(coalesce3DF.rdd.glom().collect())
```

Вывод в консоль:

```
coalesce3DF partitions count: 3
[[Row(number=1), Row(number=2)],
 [Row(number=3), Row(number=4), Row(number=5), Row(number=6)],
 [Row(number=7), Row(number=8), Row(number=9), Row(number=10)]]
```

```python
repartition3DF = inputListDF.repartition(3)
print('repartition3DF partitions count:', repartition3DF.rdd.getNumPartitions())
pprint(repartition3DF.rdd.glom().collect())
```

Вывод в консоль:

```
repartition3DF partitions count: 3
[[Row(number=5), Row(number=6), Row(number=7), Row(number=9)],
 [Row(number=1), Row(number=2), Row(number=4)],
 [Row(number=3), Row(number=8), Row(number=10)]]
```

Теперь при использовании обоих способов партиции более сбалансированы.
Перемешивание элементов при ```repartition``` выражено еще ярче.

Функции ```repartition``` вместо количества партиций можно передать название колонки,
по которой Spark будет разделять данные.

```python
repartitionColDF = inputListDF.repartition(col('number'))
print('repartitionColDF partitions count:', repartitionColDF.rdd.getNumPartitions())
pprint(repartitionColDF.rdd.glom().collect())
```

Вывод в консоль:

```
repartitionColDF partitions count: 1
[[Row(number=1),
  Row(number=2),
  Row(number=3),
  Row(number=5),
  Row(number=4),
  Row(number=6),
  Row(number=7),
  Row(number=8),
  Row(number=9),
  Row(number=10)]]
```

Можно заметить, что в данном случае Spark создал всего одну партицию.