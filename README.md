# Practising Spark Scala:

This repository contains my answers to 30 different SPARK SQL exercises. If you would like to practise yourself, you 
can find each exercise in its own package. E.G. `src/main/scala/exercise1/...md` in the same package you can find my solution in
`Exercise1.scala` etc..

### SETUP
- Spark 3.2.1
- Scala 2.12.8
- SBT 1.6.2

### How to run
- `git clone`
- `sbt clean package`
- `$SPARK_HOME/bin/spark-submit --class org.anis.boudih.exercise1.Exercise1 target/scala-2.12/scala_exercises_2.12-1.0.jar`


All kudos for creating these SPARK SQL exercises go to Jacek Laskowski.
You can find these exercises on his [Github Page](https://github.com/jaceklaskowski/spark-workshop/tree/gh-pages/exercises)
or his well known [Github Book](https://jaceklaskowski.github.io/spark-workshop/)