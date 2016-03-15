// Databricks notebook source exported at Wed, 4 Nov 2015 17:14:01 UTC
// MAGIC %md # DataFrames Scala Lab

// COMMAND ----------

// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._
import org.apache.spark.sql.DataFrame

// COMMAND ----------

// MAGIC %md **Remember**: In the notebook, you already have a `SQLContext` object, called `sqlContext`. (This is also true of a Python notebook.) However, if you need to create one yourself (e.g., for a non-notebook application), do it like this:
// MAGIC ```
// MAGIC val sc: SparkContext // some existing SparkContext object
// MAGIC val sqlContext = new org.apache.spark.sql.SQLContext(sc)
// MAGIC ```
// MAGIC 
// MAGIC **WARNING: Don't ever do this IN the notebook!**

// COMMAND ----------

// MAGIC %md We've already created a Parquet table containing popular first names by gender and year, for all years between 1880 and 2014. (This data comes from the United States Social Security Administration.) We can create a DataFrame from that data, by calling `sqlContext.read.parquet()`.
// MAGIC 
// MAGIC **NOTE**: That's the Spark 1.4 API. The API is slightly different in Spark 1.3.

// COMMAND ----------

// Spark 1.4
val df = sqlContext.read.parquet("dbfs:/mnt/training/ssn/names.parquet")

// Spark 1.3
// val df = sqlContext.parquetFile("dbfs:/mnt/training/ssn/names.parquet")


// COMMAND ----------

df.count()

// COMMAND ----------

// MAGIC %md Let's cache it, to speed things up.

// COMMAND ----------

df.cache()


// COMMAND ----------

// MAGIC %md Let's take a quick look at the first 20 items of the data.

// COMMAND ----------

df.show()

// COMMAND ----------

// MAGIC %md You can also use the `display()` helper, to get more useful (and graphable) output:

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md Take a look at the data schema, as well. Note that, in this case, the schema was read from the columns (and types) in the Parquet table.

// COMMAND ----------

df.printSchema()

// COMMAND ----------

// MAGIC %md You can create a new DataFrame that looks at a subset of the columns in the first DataFrame.

// COMMAND ----------

val firstNameDF = df.select("firstName", "year")

// COMMAND ----------

// MAGIC %md Then, you can examine the values in the `nameDF` DataFrame, using an action like `show()` or the `display()` helper:

// COMMAND ----------

display(firstNameDF)

// COMMAND ----------

// MAGIC %md You can also count the number of items in the data set...

// COMMAND ----------

firstNameDF.count()

// COMMAND ----------

// MAGIC %md ...or determine how many distinct names there are.

// COMMAND ----------

firstNameDF.select("firstName").distinct.count()

// COMMAND ----------

// MAGIC %md Let's do something a little more complicated. Let's use the original data frame to find the five most popular names for girls born in 1980.
// MAGIC 
// MAGIC **Things to Notes**
// MAGIC 
// MAGIC 1. Look closely, and you'll see a `desc` after the `orderBy()` call. `orderBy()` (which can also be invoked as `sort()`) sorts in ascending order. Adding the `desc` suffix causes the sort to be in _descending_ order.
// MAGIC 2. The Scala DataFrames API's comparison operator is `===` (_triple_ equals), not the usual `==` (_double_ equals). If you get it wrong, you'll get a Scala compiler error.

// COMMAND ----------

val df1980 = df.filter(df("year") === 1980).
                filter(df("gender") === "F").
                orderBy(df("total").desc, df("firstName")).
                select("firstName").
                limit(5)
display(df1980)

// COMMAND ----------

df1980.explain()

// COMMAND ----------

// MAGIC %md You can also use the `$` interpolator syntax to produce column references:

// COMMAND ----------

display(df.filter($"year" === 1980).
           filter($"gender" === "F").
           orderBy($"total".desc, $"firstName").
           select("firstName").
           limit(5))

// COMMAND ----------

// MAGIC %md Note that we can do the same thing using the lower-level RDD operations. However, use the DataFrame operations, when possible. In general, they're more convenient. More important, though, they allow Spark to build a query plan that can be optimized through [Catalyst](https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html).

// COMMAND ----------

// MAGIC %md ## Joins

// COMMAND ----------

// MAGIC %md Let's use two views of our data to answer this question: How popular were the top 10 female names of 2010 back in 1930?
// MAGIC 
// MAGIC Before we can do that, though, we need to define a utility function. The DataFrame SCala API doesn't support the SQL `LOWER` function. To ensure that our data matches up properly, it'd be nice to force the names to lower case before doing the match. Fortunately, it's easy to define our own `LOWER` function:

// COMMAND ----------

val lower = sqlContext.udf.register("lower", (s: String) => s.toLowerCase)

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md Okay, now we can go to work.

// COMMAND ----------

// Create a new DataFrame from the SSNA DataFrame, so that:
// - We have a lower case version of the name, for joining
// - We've weeded out the year
//
// NOTE: The aliases are necessary; otherwise, the query analyzer
// generates false equivalences between the columns.
val ssn2010 = df.filter($"year" === 2010).
                 select($"total".as("total2010"), 
                        $"gender".as("gender2010"), 
                        lower($"firstName").as("name2010"))

// Let's do the same for 1930.
val ssn1930 = df.filter($"year" === 1930).
                 select($"total".as("total1930"), 
                        $"gender".as("gender1930"), 
                        lower($"firstName").as("name1930"))

// Now, let's find out how popular the top 10 New York 2010 girls' names were in 1880.
val joined = ssn2010.join(ssn1930, ($"name2010" === $"name1930") && ($"gender2010" === $"gender1930")).
                     filter($"gender2010" === "F").
                     orderBy($"total2010".desc).
                     limit(10).
                     select($"name2010".as("name"), $"total1930", $"total2010")

display(joined)



// COMMAND ----------

// MAGIC %md ## Assignment

// COMMAND ----------

// MAGIC %md In the cell below, you'll see an empty function, `topFemaleNamesForYear`. It takes three arguments:
// MAGIC 
// MAGIC * A year
// MAGIC * A number, _n_
// MAGIC * A starting DataFrame (which will be `ssnNames`, from above)
// MAGIC 
// MAGIC It returns a new DataFrame that can be used to retrieve the top _n_ female names for that year (i.e., the _n_ names with the highest _total_ values). If there are multiple names with the same total, order those names alphabetically.
// MAGIC 
// MAGIC Write that function. To test it, run the cell _following_ the function?i.e., the one containing the `runTests()` function. (This might take a few minutes.)

// COMMAND ----------

def topFemaleNamesForYear(year: Int, n: Int, df: DataFrame): DataFrame = {
  df.filter($"year" === year).filter($"gender" === "F").orderBy($"total".desc, $"firstName").limit(n).select($"firstName")
}

// COMMAND ----------

def runTests(fcn: (Int, Int, DataFrame) => DataFrame): Unit = {
  import com.databricks.training.test.Test

  val test = Test()
  def getNames(df: DataFrame) = {
    df.collect().map { row => row(0).toString }
  }

  test.assertArrayEquals(getNames(fcn(1945, 5, df)), Array("Mary", "Linda", "Barbara", "Patricia", "Carol"), "Wrong list returned for 1945")
  test.assertArrayEquals(getNames(fcn(1970, 5, df)), Array("Jennifer", "Lisa", "Kimberly", "Michelle", "Amy"), "Wrong list returned for 1970")
  test.assertArrayEquals(getNames(fcn(1987, 5, df)), Array("Jessica", "Ashley", "Amanda", "Jennifer", "Sarah"), "Wrong list returned for 1987")
  
  // Arrays are not objects in the JVM.
  val list = getNames(fcn(1880, 5, df)).toList
  test.assertEqualsHashed(list, "dfb52c0fb408b89c9bb30b3d95f2aa33ba9888e5", "Bad result for 1880")
  
  test.printStats()
}

runTests(topFemaleNamesForYear)


// COMMAND ----------

// MAGIC %md ## Solution
// MAGIC 
// MAGIC If you're stuck, and you're really not sure how to proceed, feel free to check out the solution. You'll find it in the same folder as the lab.

// COMMAND ----------


