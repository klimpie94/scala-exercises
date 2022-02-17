# Merging two rows

Write a structured query that "merges" two rows of the same `id` (to replace `null`s).

Module: **Spark SQL**

Duration: **30 mins**

## Input Dataset

```text
val input = Seq(
  ("100","John", Some(35),None),
  ("100","John", None,Some("Georgia")),
  ("101","Mike", Some(25),None),
  ("101","Mike", None,Some("New York")),
  ("103","Mary", Some(22),None),
  ("103","Mary", None,Some("Texas")),
  ("104","Smith", Some(25),None),
  ("105","Jake", None,Some("Florida"))).toDF("id", "name", "age", "city")
scala> input.show
+---+-----+----+--------+
| id| name| age|    city|
+---+-----+----+--------+
|100| John|  35|    null|
|100| John|null| Georgia|
|101| Mike|  25|    null|
|101| Mike|null|New York|
|103| Mary|  22|    null|
|103| Mary|null|   Texas|
|104|Smith|  25|    null|
|105| Jake|null| Florida|
+---+-----+----+--------+
```

## Result

```text
scala> solution.show(truncate = false)
+---+-----+----+--------+
|id |name |age |city    |
+---+-----+----+--------+
|100|John |35  |Georgia |
|101|Mike |25  |New York|
|103|Mary |22  |Texas   |
|104|Smith|25  |null    |
|105|Jake |null|Florida |
+---+-----+----+--------+
```

<!--
## Credits

* [How to merge two rows in Spark SQL?](https://stackoverflow.com/q/61036572/1305344)

## Solution

```text
val solution = input
  .groupBy("id", "name")
  .agg(first("age", ignoreNulls = true) as "age", first("city", ignoreNulls = true) as "city")
  .orderBy("id")
```
-->