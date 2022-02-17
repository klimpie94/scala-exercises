package org.anis.boudih
package exercise4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_set, slice}

object Exercise4 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise4")
      .getOrCreate()

    import spark.implicits._
    val input = spark.range(50).withColumn("key", $"id" % 5)
    val solution = input.groupBy("key")
      .agg(collect_set("id") as "all")
      .withColumn("first_three", slice(col("all"), 1, 3))
//    input.createTempView("input")
//    val solution = spark.sql(
//      """SELECT key, collect_set(id) as all, transform(collect_set(id), x -> if (array_position(collect_set(id), x) < 3) x) as first_three
//        |  FROM input
//        |  GROUP BY key""".stripMargin)
    solution.show(truncate=false)
  }
}
