package org.anis.boudih
package exercise14

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_date, datediff, to_date}

object Exercise14 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise14")
      .getOrCreate()

    import spark.implicits._
    val dates = Seq(
      "08/11/2015",
      "09/11/2015",
      "09/12/2015").toDF("date_string")

    val solution = dates
      .withColumn("to_date", to_date($"date_string", "dd/MM/yyyy"))
      .withColumn("datediff", datediff(current_date, $"to_date"))

    solution.show()
  }
}