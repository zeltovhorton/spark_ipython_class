// Databricks notebook source exported at Wed, 4 Nov 2015 20:25:10 UTC
// MAGIC %md 
// MAGIC #Solution to Accumulators Lab

// COMMAND ----------

// MAGIC %md ## Exercise 1

// COMMAND ----------

val totalCount = sc.accumulator(1)

// COMMAND ----------

// MAGIC %md ## Exercise 2

// COMMAND ----------

rdd.cache()
val errors = rdd.filter { line => line contains "(ERROR)" }.collect()
rdd.foreach { line => totalCount += 1 }

// COMMAND ----------

// MAGIC %md ### Questions
// MAGIC 
// MAGIC How many total messages did you see?

// COMMAND ----------

println(totalCount.value)

// COMMAND ----------

// MAGIC %md How many of them were error messages?

// COMMAND ----------

println(errors.length)

// COMMAND ----------

// MAGIC %md What's the percentage of error messages to total log messages?

// COMMAND ----------

println((errors.length * 100) / totalCount.value)
