# split function with variable delimiter per row

Write a structured query that splits a column by using delimiters from another column.

**EXTRA** Write a structured query that removes empty tokens.

Module: **Spark SQL**

Duration: **15 mins**

## Input Dataset

```text
val dept = Seq(
  ("50000.0#0#0#", "#"),
  ("0@1000.0@", "@"),
  ("1$", "$"),
  ("1000.00^Test_string", "^")).toDF("VALUES", "Delimiter")
scala> dept.show
+-------------------+---------+
|             VALUES|Delimiter|
+-------------------+---------+
|       50000.0#0#0#|        #|
|          0@1000.0@|        @|
|                 1$|        $|
|1000.00^Test_string|        ^|
+-------------------+---------+
```

## Result

```text
scala> solution.show(truncate = false)
+-------------------+---------+----------------------+
|VALUES             |Delimiter|split_values          |
+-------------------+---------+----------------------+
|50000.0#0#0#       |#        |[50000.0, 0, 0, ]     |
|0@1000.0@          |@        |[0, 1000.0, ]         |
|1$                 |$        |[1, ]                 |
|1000.00^Test_string|^        |[1000.00, Test_string]|
+-------------------+---------+----------------------+
```

### Extra

```text
scala> extra.show(truncate = false)
+-------------------+---------+----------------------+----------------------+
|VALUES             |Delimiter|split_values          |extra                 |
+-------------------+---------+----------------------+----------------------+
|50000.0#0#0#       |#        |[50000.0, 0, 0, ]     |[50000.0, 0, 0]       |
|0@1000.0@          |@        |[0, 1000.0, ]         |[0, 1000.0]           |
|1$                 |$        |[1, ]                 |[1]                   |
|1000.00^Test_string|^        |[1000.00, Test_string]|[1000.00, Test_string]|
+-------------------+---------+----------------------+----------------------+
```

<!--
## Credits

* [How to use split function with variable delimiter for each row?](https://stackoverflow.com/q/62874723/1305344)

## Solution

```text
val solution = dept
  .withColumn("split_values", expr("""split(values, concat("\\", delimiter))"""))

val extra = solution
  .withColumn("extra", array_remove('split_values, "")).show(truncate = false)
```
-->