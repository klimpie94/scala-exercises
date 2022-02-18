# Exercise: Why are all fields null when querying with schema?

Write a structured query (using `spark-shell` or [Databricks Community Edition](https://community.cloud.databricks.com)) that loads a dataset with a proper schema with timestamp and prints out the rows to the standard output:

```text
2019-07-22 00:10:15,030|10.29.2.6|
2019-07-22 00:10:15,334|10.1.198.41|
2019-07-22 00:10:15,400|10.1.198.41|
2019-07-22 00:10:15,511|10.1.198.41|
2019-07-22 00:10:16,911|10.1.198.41|
```

Protip™: Use [CSV data source](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-CSVFileFormat.html)

Module: **Spark SQL**

Duration: **30 mins**

## Result

```text
scala> solution.printSchema
root
 |-- dateTime: timestamp (nullable = true)
 |-- IP: string (nullable = true)

scala> solution.show(truncate = false)
+-----------------------+-----------+
|dateTime               |IP         |
+-----------------------+-----------+
|2019-07-22 00:10:15.03 |10.29.2.6  |
|2019-07-22 00:10:15.334|10.1.198.41|
|2019-07-22 00:10:15.4  |10.1.198.41|
|2019-07-22 00:10:15.511|10.1.198.41|
|2019-07-22 00:10:16.911|10.1.198.41|
+-----------------------+-----------+
```

**NOTE**: The types!

<!--
## Solution

```text
import java.sql.Timestamp
case class Example(dateTime: Timestamp, IP: String)

import org.apache.spark.sql.Encoders
val schema = Encoders.product[Example].schema

val solution = spark
  .read
  .format("csv")
  .option("delimiter", "|")
  .option("timestampFormat", "yyyy-MM-dd HH:mm:ss,SSS")
  .schema(schema)
  .load("sample.csv")
solution.show(truncate = false)
```
-->

## Credits

* [Why are all fields null when querying with schema?](https://stackoverflow.com/q/59003568/1305344)