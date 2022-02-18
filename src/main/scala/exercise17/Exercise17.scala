package org.anis.boudih
package exercise17

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{date_add, to_date}

object Exercise17 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise17")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      (0, "2016-01-1"),
      (1, "2016-02-2"),
      (2, "2016-03-22"),
      (3, "2016-04-25"),
      (4, "2016-05-21"),
      (5, "2016-06-1"),
      (6, "2016-03-21")
    ).toDF("number_of_days", "date")

    val solution = data.withColumn("future",
      date_add(
        to_date($"date"),$"number_of_days")
    ).show()

  }
}