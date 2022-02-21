package org.anis.boudih
package exercise21

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{first, lit, concat}

object Exercise21 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise21")
      .getOrCreate()

    val exams = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("./src/main/resources/exams.csv")

    import spark.implicits._
    val solution = exams
      .withColumn("PivotColumn", concat(lit("Qid_"), $"Qid"))
      .groupBy("ParticipantID", "Assessment", "GeoTag")
      .pivot("PivotColumn")
      .agg(first("AnswerText"))

    solution.show()

  }
}