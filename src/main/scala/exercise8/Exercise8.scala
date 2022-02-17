package org.anis.boudih
package exercise8

import org.apache.spark.sql.SparkSession

object Exercise8 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise8")
      .getOrCreate()

    println(spark.version)

  }
}
