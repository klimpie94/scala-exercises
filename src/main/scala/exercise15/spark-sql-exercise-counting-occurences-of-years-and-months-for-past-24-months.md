# Exercise: Counting Occurences Of Years and Months For 24 Months From Now

Write a structured query (using `spark-shell` or [Databricks Community Edition](https://community.cloud.databricks.com)) that calculates the occurences of years and months for the past 24 months (2 years).

The query is supposed to include occurences (as `0`s) for the missing months and years (that are time gaps in the input dataset).

The query is supposed to calculate a result for the last 24 months from the date of execution. Months and years older than 24 months from now should simply be excluded from the result.

Protip™: Use the standard functions for [date and time](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$).

Module: **Spark SQL**

Duration: **30 mins**

## Input Dataset

```text
YEAR_MONTH,AMOUNT
202001,500
202001,600
201912,100
201910,200
201910,100
201909,400
201601,5000
```

```text
val sales = spark
  .read
  .option("header", true)
  .option("inferSchema", true)
  .csv("sales.csv")
scala> sales.show
+----------+------+
|YEAR_MONTH|AMOUNT|
+----------+------+
|    202001|   500|
|    202001|   600|
|    201912|   100|
|    201910|   200|
|    201910|   100|
|    201909|   400|
|    201601|  5000|
+----------+------+
```

## Result

```text
// For now being Jan 2020
scala> solution.show(100, truncate = false)
+----------+------+
|year_month|amount|
+----------+------+
|202001    |1100  |
|201912    |100   |
|201911    |0     |
|201910    |300   |
|201909    |400   |
|201908    |0     |
|201907    |0     |
...
|201804    |0     |
|201803    |0     |
+----------+------+
```

## Credits

* [Add missing monthly rows](https://stackoverflow.com/q/59845353/1305344)

<!--
## Solution

```text
val date_range = sql("""
  SELECT date_format(add_months(concat(date_format(current_date,'yyyy-MM'), '-01'), -s.id), 'yyyyMM') AS year_month
  FROM range(0,23) s
  """)
val solution = date_range.as("d")
  .join(sales, Seq("year_month"), "left")
  .groupBy($"d.year_month")
  .agg(expr("sum(nvl(amount, 0))") as "amount") // nanvl would return doubles
  .orderBy($"year_month".desc)
```
-->