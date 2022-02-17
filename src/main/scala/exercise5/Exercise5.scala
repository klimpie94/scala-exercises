package org.anis.boudih
package exercise5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, first}

case class MovieRatings(movieName: String, rating: Double)
case class MovieCritics(name: String, movieRatings: Seq[MovieRatings])

object Exercise5 {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("exercise1")
      .getOrCreate()

    import spark.implicits._

    val movies_critics = Seq(
      MovieCritics("Manuel", Seq(MovieRatings("Logan", 1.5), MovieRatings("Zoolander", 3), MovieRatings("John Wick", 2.5))),
      MovieCritics("John", Seq(MovieRatings("Logan", 2), MovieRatings("Zoolander", 3.5), MovieRatings("John Wick", 3))))
    val ratings = movies_critics.toDF

    val solution = ratings.select($"name", $"movieRatings", explode($"movieRatings.movieName") as "movieName")
      .select($"name", $"movieName", explode($"movieRatings.rating") as "rating")
      .groupBy("name")
      .pivot("movieName")
      .agg(first("rating"))
    solution.show(truncate=false)

  }
}