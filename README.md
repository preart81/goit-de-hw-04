# goit-de-hw-04. Apache Spark. Оптимізація та SparkUІ

## Завдання

- запустити три програми;
- зробити скриншоти трьох наборів Jobs;
- проаналізувати та вміти обґрунтувати наявність певної кількості Jobs у кожному наборі;
- зрозуміти, що робить функція `cache` та навіщо її використовувати.

## Рішення

### Частина 1

Запустіть код. Зробіть скриншот усіх Jobs (їх має бути 5).

```Py
from pyspark.sql import SparkSession

# Створюємо сесію Spark
spark = (
    SparkSession.builder.master("local[*]")
    .config("spark.sql.shuffle.partitions", "2")
    .appName("MyGoitSparkSandbox")
    .getOrCreate()
)

# Завантажуємо датасет
nuek_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("./csv/nuek-vuh3.csv")
)

nuek_repart = nuek_df.repartition(2)

nuek_processed = (
    nuek_repart.where("final_priority < 3")
    .select("unit_id", "final_priority")
    .groupBy("unit_id")
    .count()
)

# Ось ТУТ додано рядок
nuek_processed = nuek_processed.where("count>2")

nuek_processed.collect()

input("Press Enter to continue...5")

# Закриваємо сесію Spark
spark.stop()
```

Скріншоти Jobs:

1. [Job 0](md.media/p1_j0.png)
2. [Job 1](md.media/p1_j1.png)
3. [Job 2](md.media/p1_j2.png)
4. [Job 3](md.media/p1_j3.png)
5. [Job 4](md.media/p1_j4.png)

### Частина 2

Додамо проміжний Action — collect:

```Py
from pyspark.sql import SparkSession

# Створюємо сесію Spark
spark = (
    SparkSession.builder.master("local[*]")
    .config("spark.sql.shuffle.partitions", "2")
    .appName("MyGoitSparkSandbox")
    .getOrCreate()
)

# Завантажуємо датасет
nuek_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("./csv/nuek-vuh3.csv")
)

nuek_repart = nuek_df.repartition(2)

nuek_processed = (
    nuek_repart.where("final_priority < 3")
    .select("unit_id", "final_priority")
    .groupBy("unit_id")
    .count()
)

# Проміжний action: collect
nuek_processed.collect()

# Ось ТУТ додано рядок
nuek_processed = nuek_processed.where("count>2")

nuek_processed.collect()

input("Press Enter to continue...5")

# Закриваємо сесію Spark
spark.stop()
```

`Collect` є однією з дій (Action) які запускають Job.  
Такими діями в Spark також є: `collect()`, `count()`, `save()`, `show()`

Скріншоти Jobs:

1. [Job 0](md.media/p2_0.png)
2. [Job 1](md.media/p2_1.png)
3. [Job 2](md.media/p2_2.png)
4. [Job 3](md.media/p2_3.png)
5. [Job 4](md.media/p2_4.png)
6. [Job 5](md.media/p2_5.png)
7. [Job 6](md.media/p2_6.png)
8. [Job 7](md.media/p2_7.png)

### Частина 3

Використаємо функцію `cache` в проміжному результаті.

     ☝🏻Функція cache() в PySpark використовується для кешування (зберігання в пам'яті) даних RDD (Resilient Distributed Dataset) або DataFrame. Це дозволяє прискорити виконання подальших дій (actions) або перетворень (transformations), які працюють з тими ж даними. Кешування особливо корисне, коли ви виконуєте декілька операцій на одному й тому ж RDD або DataFrame, оскільки PySpark не буде повторно обчислювати ті самі дані.

**Як працює **`cache()`** :**

**1. Кешування в пам'яті.** Коли ви викликаєте `cache()` на RDD або DataFrame, дані зберігаються в пам'яті (RAM) у розподіленому вигляді на всіх вузлах кластера. Це дозволяє прискорити подальші обчислення, оскільки Spark не буде знову завантажувати або обчислювати ці дані.

**2. Ліниве виконання.** Виклик `cache()` не призводить до негайного виконання обчислень. Лише коли ви виконуєте дію (action), наприклад, `count()`, `collect()`, або `show()`, дані будуть обчислені та кешовані.

**3. Механізм зберігання.** За замовчуванням, `cache()` використовує пам'ять (Memory). Однак, якщо дані не поміщаються в пам'ять, Spark буде зберігати їх на диску.

**4. Контроль над кешуванням.** Коли ви використовуєте `cache()`, Spark зберігає дані з рівнем зберігання `MEMORY_ONLY`. Якщо ви хочете використовувати інші рівні зберігання, такі як `MEMORY_AND_DISK`, можна використовувати метод `persist()`.

    ☝🏻Важливо знати, що можливо зберігати як в пам'яті, так і на диску. Зберігання в пам'яті набагато більш розповсюджене, на диску — екзотика 😉

```Py
from pyspark.sql import SparkSession

# Створюємо сесію Spark
spark = (
    SparkSession.builder.master("local[*]")
    .config("spark.sql.shuffle.partitions", "2")
    .appName("MyGoitSparkSandbox")
    .getOrCreate()
)

# Завантажуємо датасет
nuek_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("./csv/nuek-vuh3.csv")
)

nuek_repart = nuek_df.repartition(2)

nuek_processed_cached = (
    nuek_repart.where("final_priority < 3")
    .select("unit_id", "final_priority")
    .groupBy("unit_id")
    .count()
    .cache()
)  # Додано функцію cache

# Проміжний action: collect
nuek_processed_cached.collect()

# Ось ТУТ додано рядок
nuek_processed = nuek_processed_cached.where("count>2")

nuek_processed.collect()

input("Press Enter to continue...5")


# Звільняємо пам'ять від Dataframe
nuek_processed_cached.unpersist()

# Закриваємо сесію Spark
spark.stop()
```

Завдяки використанню `cache` ми зменшили кількість Jobs.

Скріншоти Jobs:

1. [All jobs](md.media/p3_jobs.png)
1. [Job 0](md.media/p3_job_0.png)
1. [Job 1](md.media/p3_job_1.png)
1. [Job 2](md.media/p3_job_2.png)
1. [Job 3](md.media/p3_job_3.png)
