package org.anis.boudih
package exercise20

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, concat, first}

object Exercise20 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise20")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      (100,1,23,10),
      (100,2,45,11),
      (100,3,67,12),
      (100,4,78,13),
      (101,1,23,10),
      (101,2,45,13),
      (101,3,67,14),
      (101,4,78,15),
      (102,1,23,10),
      (102,2,45,11),
      (102,3,67,16),
      (102,4,78,18)).toDF("id", "day", "price", "units")

    data.cache()

    val prices = data
      .withColumn("daily_price", concat(lit("price_"), $"day"))
      .groupBy("id")
      .pivot("daily_price")
      .agg(first($"price"))
    val units = data
      .withColumn("daily_unit", concat(lit("unit_"), $"day"))
      .groupBy("id")
      .pivot("daily_unit")
      .agg(first($"units"))
    val solution = prices.join(units, "id").orderBy("id")

    solution.show()
  }
}
