package org.anis.boudih
package exercise22

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.first

object Exercise22 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise22")
      .getOrCreate()

    val data = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("./src/main/resources/long-dataset.csv")

    data.show()

    data
      .groupBy("key")
      .pivot("date")
      .agg(first("val1")  as "v1", first("val2")  as "v2")
      .show()
  }
}