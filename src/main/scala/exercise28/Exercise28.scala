package org.anis.boudih
package exercise28

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.first

object Exercise28 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise28")
      .getOrCreate()

    import spark.implicits._
    val data = Seq(
      (None, 0),
      (None, 1),
      (Some(2), 0),
      (None, 1),
      (Some(4), 1)).toDF("id", "group")
    val solution = data.groupBy("group").agg(first("id", true))
    solution.show()
  }
}
