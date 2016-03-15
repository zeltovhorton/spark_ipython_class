// Databricks notebook source exported at Wed, 4 Nov 2015 20:31:30 UTC
// MAGIC %md
// MAGIC # Custom Accumulators Lab
// MAGIC 
// MAGIC In this lab, we'll build a couple custom Accumulator classes. (Well, we'll show you one; then you'll build one of your own.)
// MAGIC 
// MAGIC First, some useful imports...

// COMMAND ----------

import org.apache.spark._
import scala.util.Random

// COMMAND ----------

// MAGIC %md Let's create an accumulator that's a map. We'll use it to count words. Normally, we'd just use the typical word-count RDD transformation pattern, but this alternate approach is useful when you want to do the counting as a _side effect_ of something else you're doing.

// COMMAND ----------

// MAGIC %md We'll need a custom accumulator class. This accumulator will be a map of `String` (word) to `Int` (word count).

// COMMAND ----------

class AccumWordCounts extends AccumulatorParam[Map[String,Int]] {
  /** zero() takes an initial value and converts it (if necessary)
    * into the actual initial value for the accumulator. In this case,
    * we'll just accept the caller's initial value.
    *
    * @param initial the caller's initial value.
    *
    * @return the actual initial value
    */
  def zero(initial: Map[String,Int]) = Map.empty[String, Int]
  
  /** addInPlace() is responsible for taking two maps and combining
    * them into one. Spark uses it to sum up the various node-specific
    * instances of the accumulator.
    *
    * @param m1  the first map
    * @param m2  the second map
    *
    * @return the combined map
    */
  def addInPlace(m1: Map[String, Int], m2: Map[String, Int]) = {
    val keys1 = m1.keySet
    val keys2 = m2.keySet
    
    // The keys that are common between both maps must have their
    // counts summed. The keys that are unique can just be copied
    // to the new map.
    val commonKeys = keys1 intersect keys2
    val unique1 = keys1 -- commonKeys
    val unique2 = keys2 -- commonKeys
    val commonMerged = commonKeys.map { key =>
      key -> (m1(key) + m2(key))
    }
    commonMerged.toMap ++ 
    unique1.map { key => key -> m1(key) }.toMap ++
    unique2.map { key => key -> m2(key) }.toMap
  }
}

// COMMAND ----------

// MAGIC %md Next, we need to create an instance of an accumulator of this type.

// COMMAND ----------

val countMap = sc.accumulator(Map.empty[String, Int])(new AccumWordCounts)

// COMMAND ----------

// MAGIC %md Now, let's test it with a parallelized data set, which makes it easier to validate.

// COMMAND ----------

val rdd = sc.parallelize(Array("and", "and", "then", "the", "leaves", "grass", "green", "leaves"))

// COMMAND ----------

// MAGIC %md We'll use the distributed action `foreach` to count each word. In addition, we'll convert the RDD to another RDD with upper-cased words.

// COMMAND ----------

val rdd2 = rdd.map { _.toUpperCase }
// Note that we have to add a Map here.
rdd2.foreach { word => countMap += Map(word -> 1) }


// COMMAND ----------

rdd2.collect().foreach(println)

// COMMAND ----------

// MAGIC %md What's the accumulator look like?

// COMMAND ----------

countMap.value.toSeq.sorted.foreach { case (key, value) => println(s"$key -> $value") }

// COMMAND ----------

// MAGIC %md ## Exercise
// MAGIC 
// MAGIC You're going to create an accumulator that can be used to keep track of unique occurrences of numbers. A `Set` is a useful way to keep track of uniqueness.

// COMMAND ----------

class AccumSet extends AccumulatorParam[Set[Int]] {
  def zero(/* FILL IN */) { /* FILL IN */ }
  
  def addInPlace(/* FILL IN */) { /* FILL IN */ }
}

// COMMAND ----------

val uniqueNumbers = sc.accumulator(Set.empty[Int], "my-set-accumulator")(new AccumSet)

// COMMAND ----------

// Check the initial value of the accumulator.
uniqueNumbers.value

// COMMAND ----------

// MAGIC %md To test your accumulator, we'll use 1,000,000 random numbers, with some guaranteed overlap.

// COMMAND ----------

val numbers = (1 to 1000000).map { i => Random.nextInt(100000) }

// COMMAND ----------

val rdd = sc.parallelize(numbers, 4)

// COMMAND ----------

// MAGIC %md Here's where you need to update the accumulator.

// COMMAND ----------

rdd.foreach { i => /* FILL IN */ }

// COMMAND ----------

println(s"Random numbers: ${rdd.count()}\nUnique numbers: ${uniqueNumbers.value.size}")
