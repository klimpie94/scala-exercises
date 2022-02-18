package org.anis.boudih
package exercise12

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{first, posexplode}

object Exercise12 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .getOrCreate()

    import spark.implicits._

    val input = Seq(
      Seq("a","b","c"),
      Seq("X","Y","Z")).toDF

    // FIXME: is only showing 1 line...
    val solution = input.select(posexplode($"value")).groupBy().pivot("pos").agg(first("col"))
    solution.show()

  }
}
