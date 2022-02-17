package org.anis.boudih
package exercise11

import org.apache.spark.sql.SparkSession

object Exercise11 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise11")
      .getOrCreate()

    import spark.implicits._
    val nums = Seq(Seq(1,2,3), Seq(4,5,6)).toDF("nums")
    nums.show()

    val solutionFlatMap = nums.as[Seq[Int]].flatMap(ns => ns.map(n => (ns, n))).toDF("nums", "num")
    solutionFlatMap.show()

  }
}