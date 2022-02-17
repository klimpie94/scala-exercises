# Selecting the most important rows per assigned priority

Write a structured query that selects the most important rows per assigned priority.

Module: **Spark SQL**

Duration: **15 mins**

## Input Dataset

```text
val input = Seq(
  (1, "MV1"),
  (1, "MV2"),
  (2, "VPV"),
  (2, "Others")).toDF("id", "value")
scala> input.show
+---+------+
| id| value|
+---+------+
|  1|   MV1|
|  1|   MV2|
|  2|   VPV|
|  2|Others|
+---+------+
```

## Result

```text
scala> solution.show(truncate = false)
+---+----+
| id|name|
+---+----+
|  1| MV1|
|  2| VPV|
+---+----+
```

<!--
## Credits

* [How to select the most important rows per assigned priority?](https://stackoverflow.com/q/59845044/1305344)

## Solution

```text
val priorities = Seq(
  "MV1",
  "MV2",
  "VPV",
  "Others").zipWithIndex.toDF("name", "rank")

val input = Seq(
  (1, "MV1"),
  (1, "MV2"),
  (2, "VPV"),
  (2, "Others")).toDF("id", "value")

val mins = input
  .join(priorities)
  .where($"value" === $"name")
  .groupBy("id")
  .agg(min("rank") as "min")
val solution = mins.join(priorities).where($"min" === $"rank").select("id", "name")
```
-->