// Databricks notebook source exported at Wed, 4 Nov 2015 20:31:10 UTC
// MAGIC %md
// MAGIC # Answers: Custom Accumulators Lab

// COMMAND ----------

// MAGIC %md ### Exercise Part 1

// COMMAND ----------

class AccumSet extends AccumulatorParam[Set[Int]] {
  def zero(initial: Set[Int]) = Set.empty[Int]
  
  def addInPlace(s1: Set[Int], s2: Set[Int]) = s1 union s2
}

// COMMAND ----------

// MAGIC %md ### Exercise Part 2

// COMMAND ----------

rdd.foreach { i => uniqueNumbers += Set(i) }

// COMMAND ----------


