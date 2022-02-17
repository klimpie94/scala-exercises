package org.anis.boudih
package exercise1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

object Exercise1 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise1")
      .getOrCreate()

    import spark.implicits._
    val dept = Seq(
      ("50000.0#0#0#", "#"),
      ("0@1000.0@", "@"),
      ("1$", "$"),
      ("1000.00^Test_string", "^")).toDF("VALUES", "Delimiter")

    val solution = dept.withColumn("split_values",
      expr("split(VALUES, Delimiter)")
    )

    solution.show(truncate = false)

    val extra = solution.withColumn("extra", expr("filter(split_values, x -> x != \"\")"))

    extra.show()
  }
}
