// Databricks notebook source exported at Tue, 3 Nov 2015 18:39:26 UTC
// MAGIC %md # Solutions for RDD Lab (Scala)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Question 1
// MAGIC 
// MAGIC **Does the RDD _actually_ contain the data right now?**
// MAGIC 
// MAGIC No. RDDs are _lazy_.

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Question 2
// MAGIC 
// MAGIC **Why can't you just do `baseRDD.drop(1)`?**
// MAGIC 
// MAGIC 1. There _is_ no such methods on RDDs, unlike Scala collections.
// MAGIC 2. Even if there were, RDDs are _partitioned_, and a `drop()` call would run on every node, dropping the first element of each partition. On one partition, that element would be the header, but not on the other partitions.

// COMMAND ----------

// MAGIC %md ## Exercise 1

// COMMAND ----------

// TAKE NOTE: We are deliberately only some of the fields we need for
// this lab. There's no sense dragging around more data than we need.
case class CrimeData(dateString: String,
                     timeString: String,
                     offense: String,
                     latitude: String,
                     longitude: String)
                     
val dataRDD = noHeaderRDD.map { line =>
  val cols = line.split(",")
  CrimeData(dateString = cols(10), // DISPATCH_DATE
            timeString = cols(11), // DISPATCH_TIME
            offense    = cols(6),  // TEXT_GENERAL_CODE
            latitude   = cols(7),  // POINT_X
            longitude  = cols(8))  // POINT_Y
}
dataRDD.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md ## Exercise 2

// COMMAND ----------

val groupedByOffenseRDD = dataRDD.groupBy { data => data.offense }

// What does this return? You'll need to know for the next step.
groupedByOffenseRDD.take(1).foreach(println)

// COMMAND ----------

// MAGIC %md ## Exercise 3

// COMMAND ----------

// Here's one way to do it:

val offenseCounts = groupedByOffenseRDD2.map { case (offense, items) => (offense, items.size) }.collect()
for ((offense, count) <- offenseCounts) {
  println(s"$offense: $count")
}

// COMMAND ----------

// But here's a better way. Note the use of Scala's "f" string interpolator,
// which is kind of like printf, but with compile-time type safety. For
// more information on the "f" interpolator, see
// http://docs.scala-lang.org/overviews/core/string-interpolation.html

val offenseCounts = dataRDD.map(item => (item.offense, item)).countByKey()
for ((offense, count) <- offenseCounts) {
  println(f"$offense%30s $count%5d")
}

// COMMAND ----------

// MAGIC %md ## Exercise 4

// COMMAND ----------

println(baseRDD.partitions.length)
println(groupedByOffenseRDD2.partitions.length)

// COMMAND ----------

// MAGIC %md ## Exercise 5

// COMMAND ----------

// It's okay to cache the base RDD. It's not very large. Proof:
println(s"Count of lines: ${baseRDD.count}")
val totalChars = baseRDD.map(_.length).reduce(_ + _)
println(s"Count of characters (Unicode): ${totalChars}")

// They're all about the same size, since we didn't filter any data out. However,
// since we're mostly working with the `cleanedRDD`, that's the one to cache.
cleanedRDD.cache()

// COMMAND ----------

// MAGIC %md ## Exercise 6

// COMMAND ----------

val resultRDD1 = cleanedRDD.filter(_.offense.toLowerCase contains "homicide").map { item => (item.offense, 1) }.reduceByKey(_ + _)

for ((method, count) <- resultRDD1.collect())
  println(f"$method%10s $count%d")
