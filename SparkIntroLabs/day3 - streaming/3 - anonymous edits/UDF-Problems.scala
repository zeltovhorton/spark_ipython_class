// Databricks notebook source exported at Thu, 5 Nov 2015 19:30:29 UTC
// MAGIC %md
// MAGIC # Spark Streaming and Wikipedia ![Wikipedia Logo](http://sameerf-dbc-labs.s3-website-us-west-2.amazonaws.com/data/wikipedia/images/w_logo_for_labs.png)
// MAGIC 
// MAGIC ## Discussion of Issues Defining UDFs that return BigDecimal

// COMMAND ----------

// MAGIC %md There are some issues with UDFs, when those UDFs return `BigDecimal` values. The Scala API infers the actual SQL type, mapping `BigDecimal` to a DataFrame/SQL `DecimalType`. However, it can't infer the precision and scale, so it defaults those values. When processing certain kinds of data (e.g., IPv6 addresses), this inference can cause errors. This notebook demonstrates the problem.

// COMMAND ----------

val ipAddresses = Array(
  "173.169.125.112",
  "90.11.124.2",
  "71.162.174.145",
  "75.140.23.207",
  "88.227.244.202",
  "216.223.27.25",
  "96.241.16.191",
  "2601:543:C102:361D:2893:FA1D:55F4:CEC3",
  "73.208.43.85",
  "50.171.167.113",
  "211.201.213.110",
  "1.32.74.156",
  "67.188.242.206",
  "115.187.41.3",
  "123.136.241.68",
  "181.54.26.183",
  "118.149.184.1",
  "66.190.103.230",
  "2A02:214D:8231:3800:74BA:4EB2:34DD:3FA",
  "2602:306:CD75:1F80:55ED:35E8:321D:C414",
  "94.206.48.254",
  "182.18.228.165",
  "66.94.202.246",
  "173.172.151.12",
  "174.243.71.251",
  "24.184.205.64",
  "109.60.93.67",
  "87.241.174.83",
  "78.97.142.8",
  "2601:184:4601:1E90:7D2C:98F4:78CC:3258",
  "77.98.172.76",
  "91.195.137.180",
  "109.60.93.67",
  "92.23.115.114",
  "173.206.255.12",
  "199.38.234.60",
  "2602:306:31FE:3440:CCD3:1931:3E39:7168",
  "88.166.56.222",
  "98.207.164.6",
  "24.55.38.98",
  "70.79.48.111",
  "74.101.241.38",
  "70.181.183.169",
  "1.32.74.156",
  "201.209.62.71",
  "39.47.183.218",
  "77.98.172.76",
  "73.128.107.29",
  "188.124.205.145",
  "82.89.172.24",
  "173.206.255.12",
  "39.47.183.218",
  "79.253.147.32",
  "173.20.193.118",
  "45.49.146.67",
  "2602:306:C445:1679:30CB:635A:EAB8:E553"
)

// COMMAND ----------

val df = sc.parallelize(ipAddresses).toDF("ip")

// COMMAND ----------

import com.databricks.training.helpers.InetHelpers._


// COMMAND ----------

// MAGIC %md Here's what we'd _like_ to do. With this approach, the Scala compiler (using various implicit conversions provided by Spark) _infers_ that the return type of the UDF is `DecimalType(38, 18)`.

// COMMAND ----------

import org.apache.spark.sql.functions._

val uIPNumber1 = sqlContext.udf.register("uIPNumber1", (ipAddr: String)  => {
  // parseIPAddress() returns an Option: Some on success, None on failure. It
  // handles both IPv4 address strings and IPv6 address strings.
  parseIPAddress(ipAddr).map(inetAddr => BigDecimal(ipToNumber(inetAddr)))
})

// COMMAND ----------

// MAGIC %md However, if you attempt to _use_ that UDF, any IPv6 address ends up being `null`, even though the UDF works fine.

// COMMAND ----------

df.filter($"ip" like "%:%").select($"ip", uIPNumber1($"ip").as("numericIP")).show()

// COMMAND ----------

// MAGIC %md Note that the logic _inside_ the UDF works just fine:

// COMMAND ----------

parseIPAddress("2602:306:C445:1679:30CB:635A:EAB8:E553").map(inetAddr => BigDecimal(ipToNumber(inetAddr)))

// COMMAND ----------

// MAGIC %md The problem arises because the `DecimalType` must be `DecimalType(38, 0)`, not the inferred `DecimalType(38, 18)`. There's no easy way to do that with a Scala-inferred return type. We _could_ use the (deprecated) `callUDF()` function in [org.apache.spark.sql.functions](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$), like this:

// COMMAND ----------

import org.apache.spark.sql.types._

def uIPNumber2(ipAddr: String) = parseIPAddress(ipAddr).map(inetAddr => BigDecimal(ipToNumber(inetAddr)))
df.filter($"ip" like "%:%").select($"ip", callUDF(uIPNumber2 _, DecimalType(38, 0), $"ip").as("numericIP")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC You'll note that it works, but it produces an ugly deprecation warning.
// MAGIC 
// MAGIC Another solution is to define a _Java_ UDF and use it, instead. There's a version of `sqlContext.udf.register()` that allows us to define the return type, but it only works with a Java-style UDF.
// MAGIC 
// MAGIC There's one wrinkle: The only way to call it is from SQL, because _that_ version of `register()` returns `Unit`. But that's fine, because `sqlContext.sql()` returns another DataFrame.

// COMMAND ----------

import org.apache.spark.sql.api.java.UDF1

object ParseIPToNumber extends UDF1[String, Option[BigDecimal]] {
  def call(inputValue: String) = parseIPAddress(inputValue).map(inetAddr => BigDecimal(ipToNumber(inetAddr)))
}
sqlContext.udf.register("IP2Number", ParseIPToNumber, DecimalType(38, 0))

// COMMAND ----------

df.registerTempTable("df")
val df3 = sqlContext.sql("SELECT ip, IP2Number(ip) AS numericIP FROM df WHERE ip LIKE '%:%'")
df3.show()

// COMMAND ----------


