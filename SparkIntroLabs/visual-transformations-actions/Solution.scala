// Databricks notebook source exported at Thu, 5 Nov 2015 20:55:16 UTC
// MAGIC %md
// MAGIC 
// MAGIC #![Spark Logo](http://sameerf-dbc-labs.s3-website-us-west-2.amazonaws.com/ta_Spark-logo-small.png)
// MAGIC ## Solutions to Exercises: A Visual Guide to Spark's API

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Exercise 1: Solution
// MAGIC 
// MAGIC Change the indicated line to produce squares of the original numbers.

// COMMAND ----------

val x = sc.parallelize(Array(1,2,3,4))
val y = x.map(n => n * n)
val yResults = y.collect()
println(x.collect().toNiceString)
println(yResults.toNiceString)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Exercise 2: Solution
// MAGIC Change the sample to keep even numbers.

// COMMAND ----------

val x = sc.parallelize(Array(1,2,3))
val y = x.filter(i => i % 2 == 0) // or x.filter(_ % 2 == 0)
val yResults = y.collect()
println(x.collect().toNiceString)
println(yResults.toNiceString)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Exercise 3: Solution
// MAGIC 
// MAGIC Join the RDDs so that each company's name and stock price are collected into a tuple value, whose key is the company ticker symbol.

// COMMAND ----------

val x = sc.parallelize(Array(("TWTR", "Twitter"), ("GOOG", "Google"), ("AAPL", "Apple")))
val y = sc.parallelize(Array(("TWTR", 36), ("GOOG", 532), ("AAPL", 127)))

// Add code here to perform the appropriate join join and print the result
val result = x.join(y).collect()

println(result.toNiceString)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Exercise 4: Solution
// MAGIC 
// MAGIC Create an RDD from this list, and then use `.keyBy` to create a pair RDD where:
// MAGIC 
// MAGIC * the state abbreviation is the key, and 
// MAGIC * the city + state is the value (e.g., `("NY", "New York, NY")`) ... 
// MAGIC 
// MAGIC For extra credit, add a `.map` that strips out the redundant state abbreviation to yield pairs like `("NY", "New York")`.

// COMMAND ----------

val data = Array("New York, NY", "Philadelphia, PA", "Denver, CO", "San Francisco, CA")

val x = sc.parallelize(data)
val pairs = x.keyBy(s => s.split(", ").last)
println(pairs.collect().toNiceString)

// Extra credit
val stateCityPairs = pairs.map { case (abbrev, full) =>
  (abbrev, full.split(", ").head) 
}
println(stateCityPairs.collect().toNiceString)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Exercise 5: Solution
// MAGIC 
// MAGIC Can you use `.aggregate` to collect the inputs into a plain list, so that the output of your `.aggregate` is just like that of `.collect`? How about producing a plain total, just like `.sum`? What does that tell you about the amount of data returned from `.aggregate`?

// COMMAND ----------

val x = sc.parallelize(Array(1, 2, 3, 4))

def seqOp(a: Array[Int], n: Int) = a :+ n
def combOp(a1: Array[Int], a2: Array[Int]) = a1 ++ a2

val y = x.aggregate(Array.empty[Int])(seqOp, combOp)
