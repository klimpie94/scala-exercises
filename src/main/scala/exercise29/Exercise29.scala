package org.anis.boudih
package exercise29

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, lead, when, max, count}

object Exercise29 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise29")
      .getOrCreate()

    import spark.implicits._
    val data = Seq(
         (1, 3),
         (1, 6),
         (1, 7),
         (1, 8),
         (2, 1),
         (3, 1),
         (3, 2),
         (3, 3),
         (3, 4)).toDF("id", "time")

    val windowSpec = Window.partitionBy("id").orderBy($"time".asc)
    val solution = data.withColumn("diff", col("time") - lead("time", 1).over(windowSpec)).
       withColumn("diff", when(col("diff").isNull, -1).otherwise($"diff")).
       withColumn("rank", dense_rank().over(Window.partitionBy("id").orderBy($"diff"))).
       groupBy("id","rank").agg(count("*") as "COUNT").
       groupBy("id").agg(max("COUNT"))
    solution.show()
  }
}