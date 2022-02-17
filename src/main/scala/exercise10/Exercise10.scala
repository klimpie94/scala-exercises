package org.anis.boudih
package exercise10

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_set, explode, sort_array, split}

object Exercise10 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise10")
      .getOrCreate()

    val wordsDF = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("./src/main/resources/words.csv")

    import spark.implicits._

    val words = wordsDF.withColumn("w", explode(split($"words", ",")))

    val solution = words.groupBy("w").agg(sort_array(collect_set("id")) as "ids")

    solution.show()
  }
}