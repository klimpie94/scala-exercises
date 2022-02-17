# Structs for column names and values

Write a structured query that "transpose" a dataset so a new dataset uses column names and values from a struct column.

Module: **Spark SQL**

Duration: **30 mins**

## Input Dataset

```text
case class MovieRatings(movieName: String, rating: Double)
case class MovieCritics(name: String, movieRatings: Seq[MovieRatings])
val movies_critics = Seq(
  MovieCritics("Manuel", Seq(MovieRatings("Logan", 1.5), MovieRatings("Zoolander", 3), MovieRatings("John Wick", 2.5))),
  MovieCritics("John", Seq(MovieRatings("Logan", 2), MovieRatings("Zoolander", 3.5), MovieRatings("John Wick", 3))))
val ratings = movies_critics.toDF
scala> ratings.show(truncate = false)
+------+--------------------------------------------------+
|name  |movieRatings                                      |
+------+--------------------------------------------------+
|Manuel|[[Logan, 1.5], [Zoolander, 3.0], [John Wick, 2.5]]|
|John  |[[Logan, 2.0], [Zoolander, 3.5], [John Wick, 3.0]]|
+------+--------------------------------------------------+
```

## Result

```text
scala> solution.show(truncate = false)
+------+-----+---------+---------+
|name  |Logan|Zoolander|John Wick|
+------+-----+---------+---------+
|Manuel|1.5  |3.0      |2.5      |
|John  |2.0  |3.5      |3.0      |
+------+-----+---------+---------+
```

<!--
## Credits

* [How to use structs for column names and values?](https://stackoverflow.com/q/44785583/1305344)

## Solution

```text
val ratingsCount = ratings.
  withColumn("size", size($"movieRatings")).
  select(max("size")).
  as[Int].
  head

val names_ratings = (0 until ratingsCount).
  foldLeft(ratings) { case (ds, counter) => ds.
    withColumn(s"name_$counter", $"movieRatings"(counter)("movieName")).
    withColumn(s"rating_$counter", $"movieRatings"(counter)("rating")) }

val movieColumns = names_ratings.
  columns.
  drop(1).
  filter(name => name.startsWith("name")).
  map(col)
val movieNames = names_ratings.select(movieColumns: _*).head.toSeq.map(_.toString)
val ratingNames = movieNames.indices.map(idx => s"rating_$idx")
val cols = movieNames.zip(ratingNames).map { case (movie, rn) =>
  col(rn) as movie
}
val solution = names_ratings.select(($"name" +: cols): _*)
```
-->