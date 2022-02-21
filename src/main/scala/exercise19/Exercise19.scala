package org.anis.boudih
package exercise19

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, collect_list}

object Exercise19 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise19")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      (0, "A", 223, "201603", "PORT"),
      (0, "A", 22, "201602", "PORT"),
      (0, "A", 422, "201601", "DOCK"),
      (1, "B", 3213, "201602", "DOCK"),
      (1, "B", 3213, "201601", "PORT"),
      (2, "C", 2321, "201601", "DOCK")
    ).toDF("id","type", "cost", "date", "ship")

    val solution1 = data.groupBy("id", "type")
      .pivot("date")
      .agg(avg("cost"))
      .orderBy("id", "type")

    solution1.show()

    val solution2 = data
      .groupBy("id", "type")
      .pivot("date")
      .agg(collect_list("ship"))
      .orderBy("id", "type")

    solution2.show()

  }
}
