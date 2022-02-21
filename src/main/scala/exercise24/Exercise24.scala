package org.anis.boudih
package exercise24

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{min, max}

object Exercise24 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise24")
      .getOrCreate()

    val salaries = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("./src/main/resources/salaries.csv")

    val windowSpec = Window
      .partitionBy("department")
      .orderBy("id")
      .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    import spark.implicits._
    val solution = salaries
      .withColumn("diff", max("salary").over(windowSpec).-($"salary"))

    solution.show()

  }
}
