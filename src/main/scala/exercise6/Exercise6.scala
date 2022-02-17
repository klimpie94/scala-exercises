package org.anis.boudih
package exercise6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max}

object Exercise6 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise6")
      .getOrCreate()

    import spark.implicits._

    val input = Seq(
      ("100","John", Some(35),None),
      ("100","John", None,Some("Georgia")),
      ("101","Mike", Some(25),None),
      ("101","Mike", None,Some("New York")),
      ("103","Mary", Some(22),None),
      ("103","Mary", None,Some("Texas")),
      ("104","Smith", Some(25),None),
      ("105","Jake", None,Some("Florida"))).toDF("id", "name", "age", "city")

    val solution = input.groupBy("id", "name")
      .agg(max("age").as("age"),
        max("city").as("city"))
      .orderBy("id")

    solution.show()
  }
}