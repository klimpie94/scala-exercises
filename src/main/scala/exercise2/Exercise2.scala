package org.anis.boudih
package exercise2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object Exercise2 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise2")
      .getOrCreate()

    import spark.implicits._
    val input = Seq(
      (1, "MV1"),
      (1, "MV2"),
      (2, "VPV"),
      (2, "Others")).toDF("id", "value")

    val solution = input.groupBy("id")
      .agg(expr("first(value)"))
      .select($"id", $"first(value)".alias("value"))
      .show()
  }
}
