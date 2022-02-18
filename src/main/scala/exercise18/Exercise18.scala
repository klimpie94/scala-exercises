package org.anis.boudih
package exercise18

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object Exercise18 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise18")
      .getOrCreate()

    val my_upper = udf((s: String) => s match {
    case s:String => s.toUpperCase()
    case _ => ""
    })

    import spark.implicits._
    val words = Seq(
      "i",
      "am",
      "anis").toDF("words")

    words.withColumn("Wupper", my_upper($"words")).show()
  }
}
