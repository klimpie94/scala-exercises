# Exercise: Using rollup Operator for Total and Average Salaries by Department and Company-Wide

Write a structured query that calculates total and average salaries by department and company-wide (using [rollup](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset) multi-dimentional aggregation operator).

Module: **Spark SQL**

Duration: **15 mins**

## Input Dataset

```text
id,name,department,salary
1,Hunter Fields,IT,15
2,Leonard Lewis,Support,81
3,Jason Dawson,Support,90
4,Andre Grant,Support,25
5,Earl Walton,IT,40
6,Alan Hanson,IT,24
7,Clyde Matthews,Support,31
8,Josephine Leonard,Support,1
9,Owen Boone,HR,27
10,Max McBride,IT,75
```

```text
val salaries = spark
  .read
  .option("header", true)
  .option("inferSchema", true)
  .csv("salaries.csv")
scala> salaries.show
+---+-----------------+----------+------+
| id|             name|department|salary|
+---+-----------------+----------+------+
|  1|    Hunter Fields|        IT|    15|
|  2|    Leonard Lewis|   Support|    81|
|  3|     Jason Dawson|   Support|    90|
|  4|      Andre Grant|   Support|    25|
|  5|      Earl Walton|        IT|    40|
|  6|      Alan Hanson|        IT|    24|
|  7|   Clyde Matthews|   Support|    31|
|  8|Josephine Leonard|   Support|     1|
|  9|       Owen Boone|        HR|    27|
| 10|      Max McBride|        IT|    75|
+---+-----------------+----------+------+
```

NOTE: Use [Online Generate Test Data](http://www.convertcsv.com/generate-test-data.htm) for more sophisticated datasets in CSV or JSON format.

## Result

```text
+----------+---+----+
|department|sum| avg|
+----------+---+----+
|      null|409|40.9|
|   Support|228|45.6|
|        IT|154|38.5|
|        HR| 27|27.0|
+----------+---+----+
```

<!--
## Solution

```scala
val solution = salaries.rollup('department).agg(sum('salary) as "sum", avg('salary) as "avg")
solution.show(truncate = false)
```

-->