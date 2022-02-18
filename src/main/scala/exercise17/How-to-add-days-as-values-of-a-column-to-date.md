# Exercise: How to add days (as values of a column) to date?

Write a structured query (using `spark-shell` or [Databricks Community Edition](https://community.cloud.databricks.com)) that adds a given number of days (from one column) to a date (from another column) and prints out the rows to the standard output.

```text
scala> data.show(false)
+--------------+----------+
|number_of_days|date      |
+--------------+----------+
|0             |2016-01-1 |
|1             |2016-02-2 |
|2             |2016-03-22|
|3             |2016-04-25|
|4             |2016-05-21|
|5             |2016-06-1 |
|6             |2016-03-21|
+--------------+----------+
```

Protip™: Use [date_add](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) standard function

Module: **Spark SQL**

Duration: **30 mins**

## Input Dataset

```text
val data = Seq(
  (0, "2016-01-1"),
  (1, "2016-02-2"),
  (2, "2016-03-22"),
  (3, "2016-04-25"),
  (4, "2016-05-21"),
  (5, "2016-06-1"),
  (6, "2016-03-21")
).toDF("number_of_days", "date")
```

## Result

```text
scala> solution.printSchema
root
 |-- number_of_days: integer (nullable = false)
 |-- date: string (nullable = true)
 |-- future: date (nullable = true)

scala> solution.show(truncate = false)
+--------------+----------+----------+
|number_of_days|      date|    future|
+--------------+----------+----------+
|             0| 2016-01-1|2016-01-01|
|             1| 2016-02-2|2016-02-03|
|             2|2016-03-22|2016-03-24|
|             3|2016-04-25|2016-04-28|
|             4|2016-05-21|2016-05-25|
|             5| 2016-06-1|2016-06-06|
|             6|2016-03-21|2016-03-27|
+--------------+----------+----------+
```

<!--
## Solution

```text
val solution = data.withColumn("future", expr("date_add(date, number_of_days)"))
solution.show(truncate = false)

val solution = data.selectExpr("*", "date_add(date, number_of_days) as future")
solution.show(truncate = false)
```
-->

## Credits

* [How to add days (as values of a column) to date?](https://stackoverflow.com/q/49800188/1305344)