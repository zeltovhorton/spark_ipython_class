# Databricks notebook source exported at Wed, 4 Nov 2015 17:14:41 UTC
# MAGIC %md ## Solution
# MAGIC 
# MAGIC Just cut and paste the function, below, into your lab.

# COMMAND ----------

def top_female_names_for_year(year, n, df):
  return df.filter(df.year == year).filter(df.gender == "F").select("firstName", "total").orderBy(df.total.desc(), df.firstName).limit(n)

