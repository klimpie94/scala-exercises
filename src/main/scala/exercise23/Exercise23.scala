package org.anis.boudih
package exercise23

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.rank

object Exercise23 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise23")
      .getOrCreate()

    val books = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("./src/main/resources/books.csv")

    import spark.implicits._
    val windowSpec = Window.partitionBy("genre").orderBy($"quantity".desc)
    val solution = books
      .withColumn("rank", rank().over(windowSpec))
      .where("rank < 3")

    solution.show()

  }
}