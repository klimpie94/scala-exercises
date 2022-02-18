package org.anis.boudih
package exercise13

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, expr, max, regexp_replace}
import org.apache.spark.sql.types.IntegerType

object Exercise13 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise13")
      .getOrCreate()

    val countriesDF = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("./src/main/resources/countries.csv")

    import spark.implicits._

    val window = Window.partitionBy("country")
    val solution = countriesDF
      .withColumn("population", regexp_replace($"population", " ", "").cast(IntegerType))
      .withColumn("maxPop", max("population").over(window))
      .where(expr("maxPop == population"))
      .drop("maxPop")

    solution.show()

  }
}