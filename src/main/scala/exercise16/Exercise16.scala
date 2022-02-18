package org.anis.boudih
package exercise16

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object Exercise16 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise16")
      .getOrCreate()

    val schema = StructType(Array(
      StructField("datetime",TimestampType,true),
      StructField("IP",StringType,true)
    ))

    val sales = spark
      .read
      .option("header", false)
      .option("delimiter", "|")
      .option("enforceSchema", true)
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss,SSS")
      .schema(schema)
      .csv("./src/main/resources/IP.csv")

    sales.printSchema()
    sales.show(truncate = false)

  }
}
