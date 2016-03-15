// Databricks notebook source exported at Thu, 5 Nov 2015 19:30:12 UTC
// MAGIC %md
// MAGIC # Spark Streaming and Wikipedia ![Wikipedia Logo](http://sameerf-dbc-labs.s3-website-us-west-2.amazonaws.com/data/wikipedia/images/w_logo_for_labs.png)
// MAGIC 
// MAGIC ## Solutions to Exercises and Questions

// COMMAND ----------

// MAGIC %md
// MAGIC ### Question 1
// MAGIC 
// MAGIC _How it is possible, then, that we're ending up with Parquet part files that contain no data?_
// MAGIC 
// MAGIC The `rdd.isEmpty` call determines whether the _entire_ RDD is empty. It's possible for the RDD to have data, while still having some empty _partitions_. When an RDD is written to a Parquet file, each partition is written to a separate Parquet `part` file. If an RDD has some partitions with data and other partitions that are empty, you'll end up with `part` files that have no data.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Question 2
// MAGIC 
// MAGIC _How might you increase the number of edit records in each partition of the Parquet file? What configuration item or items might help here?_
// MAGIC 
// MAGIC You could _increase_ the batch interval value, so that Spark Streaming buffers up more data per batch.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Exercise 1 (OPTIONAL: Intermediate)
// MAGIC 
// MAGIC _Using the Databricks Pie Chart, graph anonymous vs. authenticated vs. robot edits, so you can visualize the percentage of edits made by robots, by authenticated users, and by anonymous users._

// COMMAND ----------

// Use when() and otherwise() to add a "type" column we can use for aggregation.
val typed = df2.select(
  when($"robot" === true, "robot").
  when($"anonymous" === true, "anon").
  otherwise("logged in").
  as("type")
)
val groupedByType = typed.groupBy("type").count()
display(groupedByType)

// COMMAND ----------

// MAGIC %md You can also use SQL:

// COMMAND ----------

df2.registerTempTable("df2")
val typed = sqlContext.sql("""|SELECT CASE
                              |WHEN robot = true THEN 'robot'
                              |WHEN anonymous = true THEN 'anon'
                              |ELSE 'logged-in'
                              |END AS type FROM df2""".stripMargin)
val groupedByType = typed.groupBy("type").count()
display(groupedByType)


// COMMAND ----------

// MAGIC %md
// MAGIC ### Exercise 2
// MAGIC 
// MAGIC _Run a query that counts the number of instances of each IP address, displaying each IP address and the number of edits associated with it._

// COMMAND ----------

display(anonDF.select($"user").groupBy($"user").count().orderBy($"count".desc))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Exercise 3
// MAGIC 
// MAGIC _Remove the United States, Canada and the UK from the data and re-plot the results, to see who's editing English Wikipedia entries from countries where English is not the primary language._

// COMMAND ----------

display(geocoded2.filter(($"countryCode3" !== "USA") && ($"countryCode3" !== "GBR") && ($"countryCode3" !== "CAN")))
