package org.anis.boudih
package exercise3

import org.apache.spark.sql.SparkSession

object Exercise3 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise1")
      .getOrCreate()

    import spark.implicits._
    val input = Seq(
      ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
      ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
      ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
      ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
      ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
      ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
      ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
      ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
      ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
      ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
      ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
      ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001",  2),
      ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001",  2),
      ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
      ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
      ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
      ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
      ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
      ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2),
      ("05:49:56.604908", "10.0.0.3.5001",  "10.0.0.2.54880", 2))
      .toDF("column0", "column1", "column2", "label")

    input.createTempView("input")
    val solution = spark.sql(
      """SELECT column0, column1, column2, label, COUNT(column1, column2)
        | OVER (PARTITION BY column1, column2) AS count
        |  FROM input""".stripMargin)
    solution.show()
  }
}
