package org.anis.boudih
package exercise7

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, col, explode, lit, struct}
import org.apache.spark.sql.types.StructType

object Exercise7 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise7")
      .getOrCreate()

    val input = spark.read.option("multiline", "true").json("./src/main/resources/input.json")

    val days = input
      .schema
      .fields
      .filter(_.name == "hours")
      .head
      .dataType
      .asInstanceOf[StructType]
      .fieldNames

    import spark.implicits._

    val solution = input
      .select(
        $"business_id",
        $"full_address",
        explode(
          array(
            days.map(d => struct(
              lit(d).as("day"),
              col(s"hours.$d.open").as("open_time"),
              col(s"hours.$d.close").as("close_time")
            )): _*
          )
        )
      )
      .select($"business_id", $"full_address", $"col.*")

    solution.show()
  }
}
