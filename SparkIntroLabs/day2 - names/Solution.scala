// Databricks notebook source exported at Wed, 4 Nov 2015 17:15:19 UTC
// MAGIC %md ## Solution
// MAGIC 
// MAGIC Just cut and paste the function, below, into your lab.

// COMMAND ----------

def topFemaleNamesForYear(year: Int, n: Int, df: DataFrame): DataFrame = {
  df.filter($"year" === year).
     filter($"gender" === "F").
     orderBy($"total".desc, $"firstName").
     limit(n).
     select("firstName")
}

// COMMAND ----------

sqlContext

// COMMAND ----------


