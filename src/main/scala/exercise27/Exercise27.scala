package org.anis.boudih
package exercise27

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, percent_rank, when}

object Exercise27 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise27")
      .getOrCreate()

    val salaries = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("./src/main/resources/salaries2.csv")

    import spark.implicits._
    val windowSpec = Window.orderBy($"Salary")
    val solution = salaries
      .withColumn("percent_rank", percent_rank().over(windowSpec))
      .withColumn("Percentage",
        when($"percent_rank" > 0.7, "high")
          .when($"percent_rank" > 0.4, "average")
      .otherwise("low"))

    solution.show()

  }
}