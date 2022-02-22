package org.anis.boudih
package exercise26

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.concat_ws

object Exercise26 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise26")
      .getOrCreate()

    import spark.implicits._
    val words = Seq(Array("hello", "world")).toDF("words")
    val solution = words.withColumn("solution", concat_ws(" ",$"words"))
    solution.show()
  }
}
