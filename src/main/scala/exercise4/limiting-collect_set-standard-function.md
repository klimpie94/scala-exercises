# Limiting collect_set Standard Function

Write a structured query that limits [collect_set](http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html) standard function.

Module: **Spark SQL**

Duration: **15 mins**

## Input Dataset

```text
val input = spark.range(50).withColumn("key", $"id" % 5)
scala> input.show
+---+---+
| id|key|
+---+---+
|  0|  0|
|  1|  1|
|  2|  2|
|  3|  3|
|  4|  4|
|  5|  0|
|  6|  1|
|  7|  2|
|  8|  3|
|  9|  4|
| 10|  0|
| 11|  1|
| 12|  2|
| 13|  3|
| 14|  4|
| 15|  0|
| 16|  1|
| 17|  2|
| 18|  3|
| 19|  4|
+---+---+
only showing top 20 rows
```

## Result

```text
scala> solution.show(truncate = false)
+---+--------------------------------------+----------------+
|key|all                                   |only_first_three|
+---+--------------------------------------+----------------+
|0  |[0, 15, 30, 45, 5, 20, 35, 10, 25, 40]|[0, 15, 30]     |
|1  |[1, 16, 31, 46, 6, 21, 36, 11, 26, 41]|[1, 16, 31]     |
|3  |[33, 48, 13, 38, 3, 18, 28, 43, 8, 23]|[33, 48, 13]    |
|2  |[12, 27, 37, 2, 17, 32, 42, 7, 22, 47]|[12, 27, 37]    |
|4  |[9, 19, 34, 49, 24, 39, 4, 14, 29, 44]|[9, 19, 34]     |
+---+--------------------------------------+----------------+
```

<!--
## Credits

* [How to limit functions.collect_set in Spark SQL?](https://stackoverflow.com/q/38730912/1305344)

## Solution

```text
// Use slice standard function
val solution = input
  .groupBy("key")
  .agg(collect_set("id") as "all")
  .withColumn("only_first_three", slice('all, 1, 3))
```
-->