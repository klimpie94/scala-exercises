package org.anis.boudih
package exercise25

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.sum

object Exercise25 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise25")
      .getOrCreate()

    val items = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("./src/main/resources/items.csv")

    items.show()

    val windowSpec = Window
      .partitionBy("department")
      .orderBy("time")
      .rowsBetween(Window.unboundedPreceding,Window.currentRow)
    val solution = items
      .withColumn("running_total", sum("items_sold")
        .over(windowSpec))
    solution.show()
  }
}