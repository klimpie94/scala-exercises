package org.anis.boudih
package exercise7.exercise9

import org.apache.spark.sql.SparkSession

object Exercise9 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise9")
      .getOrCreate()

    val csvDF = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("./src/main/resources/mnm_dataset.csv").show()
  }
}
