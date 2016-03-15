// Databricks notebook source exported at Thu, 5 Nov 2015 20:55:01 UTC
// MAGIC %md
// MAGIC 
// MAGIC #![Spark Logo](http://sameerf-dbc-labs.s3-website-us-west-2.amazonaws.com/ta_Spark-logo-small.png)
// MAGIC # A Visual Guide to Spark's API
// MAGIC ## Time to complete: 30 minutes
// MAGIC 
// MAGIC This lab will introduce you to using Apache Spark 1.4 with the Scala API. We will explore common transformations and actions including:
// MAGIC 
// MAGIC * Actions: collect, count, partitions, reduce, aggregate, max, min, sum, mean, variance, stdev, countByKey, saveAsTextFile
// MAGIC * Transformations + MISC operations: map, filter, flatMap, groupBy, groupByKey, napPartitions, mapPartitionsWithIndex, sample, union, join, distinct, coalese, keyBy, partitionBy, zip
// MAGIC 
// MAGIC 
// MAGIC Note that these images were inspired by Jeff Thomson's 67 "PySpark images".

// COMMAND ----------

// We'll be dealing with a lot of arrays in this lab. Unfortunately,
// courtesy of the JVM, the default array-to-string representation is
// something unhelpful (like "[I@68d1251", for an array of integers).
// The following import adds a toNiceString function to (most) arrays,
// which helps get around this problem.
import com.databricks.training.helpers.Enrichments.EnrichedArray

// Used as a separator between printed output and REPL results.
val Separator = "-" * 40

// For tests
val test = new com.databricks.training.test.Test


// COMMAND ----------

// MAGIC %md ###Collect
// MAGIC 
// MAGIC Action / To Driver: Return all items in the RDD to the driver in a single list
// MAGIC 
// MAGIC Start with this action, since it is used in all of the examples.
// MAGIC 
// MAGIC ![](http://i.imgur.com/DUO6ygB.png)

// COMMAND ----------

val x = sc.parallelize(Array(1, 2, 3), 2)
val y = x.collect()

println(x.glom().collect().toNiceString)
println(y.toNiceString)
println(Separator) // separates our output from the REPL's output

// COMMAND ----------

// MAGIC %md ## Transformations
// MAGIC 
// MAGIC Create a new RDD from one or more RDDs

// COMMAND ----------

// MAGIC %md ###Map
// MAGIC 
// MAGIC Transformation / Narrow: Return a new RDD by applying a function to each element of this RDD
// MAGIC 
// MAGIC ![](http://i.imgur.com/PxNJf0U.png)

// COMMAND ----------

val x = sc.parallelize(Array("b", "a", "c"))
val y = x.map(z => (z, 1))
println(x.collect().toNiceString)
println(y.collect().toNiceString)
println(Separator)


// COMMAND ----------

// MAGIC %md 
// MAGIC ### ![](http://i.imgur.com/fsPz68O.png) Try it!
// MAGIC 
// MAGIC **Exercise 1:** Change the indicated line to produce squares of the original numbers.

// COMMAND ----------

// Lab exercise:

val x = sc.parallelize(Array(1,2,3,4))
val y = x.map(n => n) // CHANGE the lambda to take a number and returns its square
val yResults = y.collect()
println(x.collect().toNiceString)
println(yResults.toNiceString)

println(Separator)
test.assertArrayEquals(yResults, Array(1, 4, 9, 16), "Incorrect result.")
println(Separator)

// COMMAND ----------

// MAGIC %md #### Filter
// MAGIC 
// MAGIC Transformation / Narrow: Return a new RDD containing only the elements that satisfy a predicate
// MAGIC 
// MAGIC ![](http://i.imgur.com/GFyji4U.png)

// COMMAND ----------

val x = sc.parallelize(1 to 20)
val y = x.filter(x => x % 2 == 1) // keep odd values 
println(x.collect().toNiceString)
println(y.collect().toNiceString)
println(Separator)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![](http://i.imgur.com/fsPz68O.png) Try it!
// MAGIC 
// MAGIC **Exercise 2:** Change the sample to keep even numbers.

// COMMAND ----------

// Lab exercise:
val x = sc.parallelize(Array(1,2,3))
val y = x.filter(...) // add a lambda parameter to keep only even numbers
val yResults = y.collect()
println(x.collect().toNiceString)
println(yResults.toNiceString)

println(Separator)
test.assertArrayEquals(yResults, Array(2), "Incorrect result")
println(Separator)

// COMMAND ----------

// MAGIC %md ### FlatMap
// MAGIC 
// MAGIC Transformation / Narrow: Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results
// MAGIC 
// MAGIC ![](http://i.imgur.com/TsSUex8.png)

// COMMAND ----------

val x = sc.parallelize(Array(1,2,3))
val y = x.flatMap(x => Array(x, x*100, 42))
println(x.collect().toNiceString)
println(y.collect().toNiceString)
println(Separator)


// COMMAND ----------

// MAGIC %md ### GroupBy
// MAGIC 
// MAGIC Transformation / Wide: Group the data in the original RDD. Create pairs where the key is the output of a user function, and the value is all items for which the function yields this key.
// MAGIC 
// MAGIC ![](http://i.imgur.com/gdj0Ey8.png)

// COMMAND ----------

val x = sc.parallelize(Array("John", "Fred", "Anna", "James"))
val y = x.groupBy(w => w(0))
println(y.collect().toNiceString)
println(Separator)


// COMMAND ----------

// MAGIC %md ### GroupByKey
// MAGIC 
// MAGIC Transformation / Wide: Group the values for each key in the original RDD. Create a new pair where the original key corresponds to this collected group of values.
// MAGIC 
// MAGIC ![](http://i.imgur.com/TlWRGr2.png)

// COMMAND ----------

val x = sc.parallelize(Array(('B', 5), ('B', 4), ('A', 3), ('A', 2),('A', 1)))
val y = x.groupByKey()
println(x.collect().toNiceString)
println(y.collect().toNiceString)
println(Separator)

// COMMAND ----------

// MAGIC %md ### MapPartitions
// MAGIC 
// MAGIC Transformation / Narrow: Return a new RDD by applying a function to each partition of this RDD
// MAGIC 
// MAGIC ![](http://i.imgur.com/dw8QOLX.png)

// COMMAND ----------

val x = sc.parallelize(1 to 9, 2)
val y = x.mapPartitions { iterator: Iterator[Int] => Iterator(iterator.sum) }

println(x.glom().collect().toNiceString)
println(y.glom().collect().toNiceString)
println(Separator)

// COMMAND ----------

// MAGIC %md ### MapPartitionsWithIndex
// MAGIC 
// MAGIC Transformation / Narrow: Return a new RDD by applying a function to each partition of this RDD, while tracking the index of the original partition
// MAGIC 
// MAGIC ![](http://i.imgur.com/3cGvAF7.png)

// COMMAND ----------

val x = sc.parallelize(1 to 9, 2)

val y = x.mapPartitionsWithIndex { (partitionIndex, iterator) =>
  Iterator(partitionIndex, iterator.sum)
}

println(x.glom().collect().toNiceString)
println(y.glom().collect().toNiceString)
println(Separator)

// COMMAND ----------

// MAGIC %md ### Sample
// MAGIC 
// MAGIC Transformation / Narrow: Return a new RDD containing a statistical sample of the original RDD
// MAGIC 
// MAGIC ![](http://i.imgur.com/LJ56nQq.png)

// COMMAND ----------

val x = sc.parallelize(1 to 20, 2)
val y = x.sample(false, 0.4, 42)
println(x.collect().toNiceString)
println(y.collect().toNiceString)
println(Separator)

// COMMAND ----------

// MAGIC %md ### Union
// MAGIC 
// MAGIC Transformation / Narrow: Return a new RDD containing all items from two original RDDs. Duplicates are not culled.
// MAGIC 
// MAGIC ![](http://i.imgur.com/XFpbqZ8.png)

// COMMAND ----------

val x = sc.parallelize(Array(1, 2, 3), 2)
val y = sc.parallelize(Array(3, 4), 1)
val z = x.union(y)
println(z.glom().collect().toNiceString)
println(Separator)

// COMMAND ----------

// MAGIC %md ### Join
// MAGIC 
// MAGIC Transformation / Wide: Return a new RDD containing all pairs of elements having the same key in the original RDDs
// MAGIC 
// MAGIC ![](http://i.imgur.com/YXL42Nl.png)

// COMMAND ----------

val x = sc.parallelize(Array(("a", 1), ("b", 2)))
val y = sc.parallelize(Array(("a", 3), ("a", 4), ("b", 5)))
val z = x.join(y)
println(z.collect().toNiceString)
println(Separator)


// COMMAND ----------

// MAGIC %md 
// MAGIC ### ![](http://i.imgur.com/fsPz68O.png) Try it!
// MAGIC 
// MAGIC **Exercise 3:** Join the RDDs so that each company's name and stock price are collected into a tuple value, whose key is the company ticker symbol.

// COMMAND ----------

val x = sc.parallelize(Array(("TWTR", "Twitter"), ("GOOG", "Google"), ("AAPL", "Apple")))
val y = sc.parallelize(Array(("TWTR", 36), ("GOOG", 532), ("AAPL", 127)))

// Add code here to perform the appropriate join join and print the result
val result = ...

println(result.toNiceString)
println(Separator)
test.assertArrayEquals(result.sorted, Array(("AAPL", ("Apple", 127)), ("GOOG", ("Google", 532)), ("TWTR", ("Twitter", 36))), "Incorrect result.")
println(Separator)

// COMMAND ----------

// MAGIC %md ### Distinct
// MAGIC 
// MAGIC Transformation / Wide: Return a new RDD containing distinct items from the original RDD (omitting all duplicates)
// MAGIC 
// MAGIC ![](http://i.imgur.com/Vqgy2a4.png)

// COMMAND ----------

val x = sc.parallelize(Array(1, 2, 3, 3, 4))
val y = x.distinct()

println(y.collect().toNiceString)
println(Separator)


// COMMAND ----------

// MAGIC %md ### Coalesce
// MAGIC 
// MAGIC Transformation / Narrow or Wide: Return a new RDD which is reduced to a smaller number of partitions
// MAGIC 
// MAGIC ![](http://i.imgur.com/woQiM7E.png)

// COMMAND ----------

val x = sc.parallelize(Array(1, 2, 3, 4, 5), 3)
val y = x.coalesce(2)
println(x.glom().collect().toNiceString)
println(y.glom().collect().toNiceString)
println(Separator)


// COMMAND ----------

// MAGIC %md ### KeyBy
// MAGIC 
// MAGIC Transformation / Narrow: Create a Pair RDD, forming one pair for each item in the original RDD. The pair?s key is calculated from the value via a user-supplied function.
// MAGIC 
// MAGIC ![](http://i.imgur.com/nqYhDW5.png)

// COMMAND ----------

val x = sc.parallelize(Array("John", "Fred", "Anna", "James"))
val y = x.keyBy(s => s(0))
println(y.collect().toNiceString)
println(Separator)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### ![](http://i.imgur.com/fsPz68O.png) Try it!
// MAGIC 
// MAGIC **Exercise 4:** Create an RDD from this list, and then use `.keyBy` to create a pair RDD where:
// MAGIC 
// MAGIC * the state abbreviation is the key, and 
// MAGIC * the city + state is the value (e.g., `("NY", "New York, NY")`) ... 
// MAGIC 
// MAGIC For extra credit, add a `.map` that strips out the redundant state abbreviation to yield pairs like `("NY", "New York")`.

// COMMAND ----------

val data = Array("New York, NY", "Philadelphia, PA", "Denver, CO", "San Francisco, CA")

// Add code to parallelize the list to an RDD
// call .keyBy on the RDD to create an RDD of pairs
val x = sc.parallelize(data)
/* FILL THIS IN */
val result = ...
println(result.toNiceString)
println(Separator)
val expected = Array(("CA", "San Francisco, CA"), ("CO","Denver, CO"), ("NY", "New York, NY"), ("PA", "Philadelphia, PA"))
test.assertArrayEquals(result.sorted, expected, "Incorrect results.")
println(Separator)


// COMMAND ----------

// MAGIC %md ### PartitionBy
// MAGIC 
// MAGIC Transformation / Wide: Return a new RDD with the specified number of partitions, placing original items into the partition returned by a user supplied partitioner.
// MAGIC 
// MAGIC ![](http://i.imgur.com/QHDWwYv.png)

// COMMAND ----------

val x = sc.parallelize(Array(('J', "James"), ('F', "Fred"), ('A', "Anna"), ('J', "John")), 3)

class MyPartitioner extends org.apache.spark.Partitioner with Serializable {
  def getPartition(key: Any) = key match {
    case c: Char => if (c < 'H') 0 else 1
    case _       => 0
  }
  val numPartitions = 2
}

val y = x.partitionBy(new MyPartitioner)
println(x.glom().collect().toNiceString)
println(y.glom().collect().toNiceString)
println(Separator)

// COMMAND ----------

// MAGIC %md ### Zip
// MAGIC 
// MAGIC Transformation / Narrow: Return a new RDD containing pairs whose key is the item in the original RDD, and whose value is that item?s corresponding element (same partition, same index) in a second RDD
// MAGIC 
// MAGIC ![](http://i.imgur.com/5J0lg6g.png)

// COMMAND ----------

val x = sc.parallelize(Array(1, 2, 3))
val y = x.map(n => n * n)
val z = x.zip(y)

println(z.collect().toNiceString)
println(Separator)

// COMMAND ----------

// MAGIC %md ## Actions
// MAGIC 
// MAGIC Calculate a result (e.g., numeric data or creata a non-RDD data structure), or produce a side effect, such as writing output to disk

// COMMAND ----------

// MAGIC %md ### partitions
// MAGIC 
// MAGIC Action / To Driver: Return an array of partition descriptors, representing the partitions in an RDD.
// MAGIC 
// MAGIC ![](http://i.imgur.com/9yhDsVX.png)

// COMMAND ----------

val x = sc.parallelize(Array(1, 2, 3), 2)
val y = x.partitions.length

println(x.glom().collect().toNiceString)
println(s"Total partitions: $y")
println(Separator)

// COMMAND ----------

// MAGIC %md ### Reduce
// MAGIC 
// MAGIC Action / To Driver: Aggregate all the elements of the RDD by applying a user function pairwise to elements and partial results, and return a result to the driver
// MAGIC 
// MAGIC ![](http://i.imgur.com/R72uzwX.png)

// COMMAND ----------

val x = sc.parallelize(1 to 4)
val y = x.reduce { (a, b) => a + b } // or: x.reduce(_ + _)

println(x.collect().toNiceString)
println(s"sum=$y")
println(Separator)

// COMMAND ----------

// MAGIC %md ### Aggregate
// MAGIC 
// MAGIC Action / To Driver: Aggregate all the elements of the RDD by: 
// MAGIC   - applying a user function to combine elements with user-supplied objects, 
// MAGIC   - then combining those user-defined results via a second user function, 
// MAGIC   - and finally returning a result to the driver.
// MAGIC   
// MAGIC ![](http://i.imgur.com/7MLnYeh.png)

// COMMAND ----------

// MAGIC %md
// MAGIC What we're doing below is aggregating an array of integers into a tuple, consisting of the
// MAGIC (possibly reordered) original array _and_ the sum of the array's values.

// COMMAND ----------

def seqOp(data: (Array[Int], Int), item: Int) = {
  val (array, item1) = data
  (array :+ item, item1 + item)
}

def combOp(d1: (Array[Int], Int), d2: (Array[Int], Int)) = {
  val (a1, i1) = d1
  val (a2, i2) = d2
  
  (a1 ++ a2, i1 + i2)
}

val x = sc.parallelize(Array(1, 2, 3, 4))

val (array, total) = x.aggregate((Array.empty[Int], 0))(seqOp, combOp)
println(s"(${array.toNiceString}, $total)")
println(Separator)


// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### ![](http://i.imgur.com/fsPz68O.png) Try it!
// MAGIC 
// MAGIC **Exercise 5:** Can you use `.aggregate` to collect the inputs into a plain list, so that the output of your `.aggregate` is just like that of `.collect`? How about producing a plain total, just like `.sum`? What does that tell you about the amount of data returned from `.aggregate`?

// COMMAND ----------

val x = sc.parallelize(Array(1, 2, 3, 4))

// Define appropriate seqOp and combOp

def seqOp(...) = ...
def combOp(...) = ...

val y = x.aggregate(...) //add correct parameters

// ----

val result = x.collect()
// These two lines should produce the same thing (possibly with 
// the contents in different orders).
println(result.toNiceString)
println(y.toNiceString)

println(Separator)
test.assertArrayEquals(result.sorted, y.sorted, "Incorrect result.")
println(Separator)

// COMMAND ----------

// MAGIC %md ### Max, Min, Sum, Mean, Variance, Stdev
// MAGIC 
// MAGIC Action / To Driver: Compute the respective function (maximum value, minimum value, sum, mean, variance, or standard deviation) from a numeric RDD
// MAGIC 
// MAGIC ![](http://i.imgur.com/HUCtib1.png)

// COMMAND ----------

val x = sc.parallelize(Array(2, 4, 1))
println(x.collect().toNiceString)
println(s"max=${x.max()}, min=${x.min()}, sum=${x.sum()}, mean=${x.mean()}, variance=${x.variance()}, stdev=${x.stdev()}")
println(Separator)

// COMMAND ----------

// MAGIC %md ### CountByKey
// MAGIC 
// MAGIC Action / To Driver: Return a map of keys and counts of their occurrences in the RDD
// MAGIC 
// MAGIC ![](http://i.imgur.com/jvQTGv6.png)

// COMMAND ----------

val x = sc.parallelize(Array(('J', "James"), ('F', "Fred"), ('A', "Anna"), ('J', "John")))
val y = x.countByKey()

println(y)
println(Separator)

// COMMAND ----------

// MAGIC %md ### SaveAsTextFile
// MAGIC 
// MAGIC Action / Distributed: Save the RDD to the filesystem indicated in the path
// MAGIC 
// MAGIC ![](http://i.imgur.com/Tb2Q9mG.png)

// COMMAND ----------

// NOTE: We're using a random number here, just to ensure that there are no clashes.
import scala.util.Random
val id = Random.nextInt(100)
val Dir = "/tmp/demo"
val Output = s"$Dir/$id"

dbutils.fs.rm(Output, recurse=true)
val x = sc.parallelize(1 to 100, 4)
x.saveAsTextFile(Output)

println(Separator)
println("Contents of directory:")
dbutils.fs.ls(Output).foreach(fileInfo => println(fileInfo.name))

val y = sc.textFile(Output)
println(Separator)
println("Contents of text file:")
println(y.collect().toNiceString)
println(Separator)


// COMMAND ----------


