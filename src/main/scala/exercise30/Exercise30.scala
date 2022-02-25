package org.anis.boudih
package exercise30

import org.apache.spark.sql.functions.{avg, sum}
import org.apache.spark.sql.{SparkSession, functions}

object Exercise30 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("exercise30")
      .getOrCreate()

    val salaries = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("./src/main/resources/salaries.csv")

    val solution = salaries
      .rollup("department")
      .agg(sum("salary") as "sum", avg("salary") as "avg")
    solution.show(truncate = false)

  }
}
