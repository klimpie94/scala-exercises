package org.anis.boudih
package exercise15

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{current_date, expr, to_date}
import org.apache.spark.sql.types.StringType

object Exercise15 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise15")
      .getOrCreate()

    val sales = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("./src/main/resources/sales.csv")

    import spark.implicits._

    val date_range = spark.sql("""
                          SELECT date_format(add_months(concat(date_format(current_date,'yyyy-MM'), '-01'), -s.id), 'yyyyMM') AS year_month
                          FROM range(0,36) s
                          """)
    val solution = sales
      .join(date_range.as("dates"), date_range("year_month") ===  sales("YEAR_MONTH"), "rightouter")
      .groupBy("dates.year_month")
      .agg(expr("sum(nvl(AMOUNT, 0))"))
      .orderBy($"dates.year_month".desc)

    solution.show()

  }
}