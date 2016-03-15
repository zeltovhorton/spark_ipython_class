// Databricks notebook source exported at Tue, 16 Feb 2016 14:58:15 UTC
// MAGIC %md
// MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

// COMMAND ----------

// MAGIC %md
// MAGIC # Wikipedia: Exploratory Data Analysis (EDA) using DataFrames
// MAGIC  
// MAGIC This lab explores English wikipedia articles using `DataFrames`.  You'll learn about `DataFrame`, `Column`, and `GroupedData` objects and the `functions` package.  After you complete this lab you should be able to use much of the functionality found in Spark SQL and know where to find additional reference material.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Load the data and start the EDA
// MAGIC  
// MAGIC We'll be mostly using functions and objects that are found in Spark SQL.  The [Python](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.package) APIs and the [Spark SQL and DataFrame Guide](http://spark.apache.org/docs/latest/sql-programming-guide.html) are all very useful references.

// COMMAND ----------

// MAGIC %md
// MAGIC To start, work from the small sample to speed the EDA process.

// COMMAND ----------

val baseDir = "/mnt/ml-class/"
val dfSmall = sqlContext.read.parquet(baseDir + "smallwiki.parquet").cache
println(dfSmall.count)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a look at how our `DataFrame` is represented.

// COMMAND ----------

println(s"dfSmall: $dfSmall")
println(s"dfSmall.getClass: ${dfSmall.getClass}")

// COMMAND ----------

println(dfSmall.schema)
dfSmall.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC We can see that our schema is made up of a `StructType` that contains `StructField` objects.  These `StructField` objects have several properties including: name, data type, whether they can be null, and metadata.  Note that the list of fields for a `StructType` can also include other `StructType` objects to allow for nested structures.

// COMMAND ----------

// MAGIC %md
// MAGIC Next, we'll create an example `DataFrame` where we specify the schema using `StructType` and `StructField`.  Schema can also be inferred by Spark during `DataFrame` creation.

// COMMAND ----------

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, BooleanType, StringType, LongType, MetadataBuilder}
 
val titleMetadata = new MetadataBuilder()
  .putString("language", "english")
  .build()
val schema = StructType(Array(StructField("title", StringType, false, titleMetadata),
                     StructField("numberOfEdits", LongType),
                     StructField("redacted", BooleanType)))
 
val exampleData = sc.parallelize(Seq(Row("Baade's Window", 100L, false),
                                     Row("Zenomia", 10L, true),
                                     Row("United States Bureau of Mines", 5280L, true)))
 
val exampleDF = sqlContext.createDataFrame(exampleData, schema)
display(exampleDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's view the schema that we created.

// COMMAND ----------

println(exampleDF.schema)
exampleDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC Our `metadata` for the `title` field has also been captured.  We might create a new `DataFrame` from this `DataFrame` using a transformer and we could pass along or modify this `metadata` in the process.

// COMMAND ----------

println(exampleDF.schema.fields(0).metadata)
println(exampleDF.schema.fields(1).metadata)

// COMMAND ----------

// MAGIC %md
// MAGIC What does a row of wikipedia data look like?  Let's take a look at the first observation.

// COMMAND ----------

println(dfSmall.first)

// COMMAND ----------

// MAGIC %md
// MAGIC What are our column names?

// COMMAND ----------

dfSmall.columns

// COMMAND ----------

// MAGIC %md
// MAGIC The text is long and obscures the rest of the data.  Let's use `drop` to remove the text.

// COMMAND ----------

println(dfSmall.drop("text").first)

// COMMAND ----------

// MAGIC %md
// MAGIC Next, let's view the text in a format that more closely resembles how it would be displayed.

// COMMAND ----------

println(dfSmall.select("text").first()(0))

// COMMAND ----------

// MAGIC %md
// MAGIC When we parsed the XML we stored `<PARSE ERROR>` as the title for any record that our XML parser couldn't handle.  Let's see how many records had errors.

// COMMAND ----------

import org.apache.spark.sql.functions.col
val errors = dfSmall.filter(col("title") === "<PARSE ERROR>")
val errorCount = errors.count()
println(errorCount)
println(errorCount.toDouble / dfSmall.count)

// COMMAND ----------

// MAGIC %md
// MAGIC We can also do the `Column` selection several different ways.

// COMMAND ----------

println(dfSmall.filter(dfSmall("title") === "<PARSE ERROR>").count)
println(dfSmall.filter($"title" === "<PARSE ERROR>").count)

// COMMAND ----------

// MAGIC %md
// MAGIC We can see that `errors` contains those items with a title that equals `<PARSE ERROR>`.  Note that we can rename our column using `.alias()` and display our `DataFrame` using `.show()`.  `alias` is a method that we are calling on a `Column` and `show` is a method called on the `DataFrame`.

// COMMAND ----------

// We could also use as instead of alias
errors.select($"title".alias("badTitle")).show(3)

// COMMAND ----------

// MAGIC %md
// MAGIC And what does an error look like?

// COMMAND ----------

println(errors.select("text").first()(0))

// COMMAND ----------

// MAGIC %md
// MAGIC Let's use some `Column` and `DataFrame` operations to inspect the `redirect_title` column.

// COMMAND ----------

dfSmall
  .select($"redirect_title".isNotNull.as("hasRedirect"))
  .groupBy("hasRedirect")
  .count
  .show

// COMMAND ----------

// MAGIC %md
// MAGIC Now, let's filter out the data that has a parse error, is a redirect, or doesn't have any text.

// COMMAND ----------

val filtered = dfSmall.filter(($"title" !== "<PARSE ERROR>") &&
                              $"redirect_title".isNull &&
                              $"text".isNotNull)
println(filtered.count)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Helpful functions
// MAGIC  
// MAGIC In addition to the functions that can be called on a `DataFrame`, `Column`, or `GroupedData`, Spark SQL also has a `functions` package that provides functions like those typically built into a database system that can be called from SQL.  This include functions for performing mathematical operations, handling dates and times, string manipulation, and more.
// MAGIC  
// MAGIC The [Python](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) APIs have good descriptions for these functions.

// COMMAND ----------

import org.apache.spark.sql.{functions => func}

// COMMAND ----------

// MAGIC %md
// MAGIC Next, we'll use the time functions to convert our timestamp into Central European Summer Time (CEST).

// COMMAND ----------

filtered.select("timestamp").show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's try applying `date_format` to see how it operates.

// COMMAND ----------

filtered
  .select($"timestamp", func.date_format($"timestamp", "MM/dd/yyyy").as("date"))
  .show(5)

// COMMAND ----------

val withDate = filtered.withColumn("date", func.date_format($"timestamp", "MM/dd/yyyy"))
withDate.printSchema
 
withDate
  .select("title", "timestamp", "date")
  .show(3)

// COMMAND ----------

// MAGIC %md
// MAGIC It seems like we want a different function for time zone manipulation and to store the object as a timestamp rather than a string.  Let's use `from_utc_timestamp` to get a timestamp object back with the correct time zone.

// COMMAND ----------

val withCEST = withDate.withColumn("cest_time", func.from_utc_timestamp($"timestamp", "Europe/Amsterdam"))
withCEST.printSchema
 
withCEST
  .select("timestamp", "cest_time")
  .show(3, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Next, let's convert the text field to lowercase.  We'll use the `lower` function for this.

// COMMAND ----------

val lowered = withCEST.select($"*", func.lower($"text").as("lowerText"))
 
println(lowered.select("lowerText").first)

// COMMAND ----------

// MAGIC %md
// MAGIC What columns do we have now?

// COMMAND ----------

lowered.columns

// COMMAND ----------

// MAGIC %md
// MAGIC Let's go ahead and drop the columns we don't want and rename `lowerText` to `text`.

// COMMAND ----------

val parsed = lowered
  .drop("text")
  .drop("timestamp")
  .drop("date")
  .withColumnRenamed("lowerText", "text")
 
println("Columns:")
parsed.columns.foreach(println)
 
println("\n\n" + parsed.select("text").first)

// COMMAND ----------

// MAGIC %md
// MAGIC Next, let's convert our text into a list of words so that we can perform some analysis at the word level.  For this we will use a feature transformer called `RegexTokenizer` which splits up strings into tokens (words in our case) based on a split pattern.  We'll split our text on anything that matches one or more non-word characters.

// COMMAND ----------

import org.apache.spark.ml.feature.RegexTokenizer
 
val tokenizer = new RegexTokenizer()
  .setInputCol("text")
  .setOutputCol("words")
  .setPattern("\\W+")
val wordsDF = tokenizer.transform(parsed)

// COMMAND ----------

wordsDF.select("words").first

// COMMAND ----------

// MAGIC %md
// MAGIC There are some very common words in our list of words which won't be that useful for our later analysis.  We'll create a UDF to remove them.
// MAGIC  
// MAGIC [StopWordsRemover](http://spark.apache.org/docs/latest/ml-features.html#stopwordsremover) is implemented for Scala but not yet for Python.  We'll use the same [list](http://ir.dcs.gla.ac.uk/resources/linguistic_utils/stop_words) of stop words it uses to build a user-defined function (UDF).

// COMMAND ----------

val stopWords = sc.textFile("/mnt/ml-class/stop_words.txt").collect.toSet

// COMMAND ----------

// MAGIC %md
// MAGIC Create our function for removing words.

// COMMAND ----------

import scala.collection.mutable.WrappedArray
 
val stopWordsBroadcast = sc.broadcast(stopWords)
 
def isDigitOrUnderscore(c: Char) = {
    Character.isDigit(c) || c == '_'
}
 
def keepWord(word: String) = word match {
    case x if x.length < 3 => false
    case x if stopWordsBroadcast.value(x) => false
    case x if x exists isDigitOrUnderscore => false
    case _ => true
}
 
def removeWords(words: WrappedArray[String]) = {
    words.filter(keepWord(_))
}

// COMMAND ----------

// MAGIC %md
// MAGIC Test the function locally.

// COMMAND ----------

removeWords(Array("test", "cat", "do343", "343", "spark", "the", "and", "hy-phen", "under_score"))

// COMMAND ----------

// MAGIC %md
// MAGIC Create a UDF from our function.

// COMMAND ----------

import org.apache.spark.sql.functions.udf
val removeWordsUDF = udf { removeWords _ }

// COMMAND ----------

// MAGIC %md
// MAGIC Register this function so that we can call it later from another notebook.  Note that in Scala `register` also returns a `udf` that we can use, so we could have combined the above step into this step.

// COMMAND ----------

sqlContext.udf.register("removeWords", removeWords _)

// COMMAND ----------

// MAGIC %md
// MAGIC Apply our function to the `wordsDF` `DataFrame`.

// COMMAND ----------

val noStopWords = wordsDF
  .withColumn("noStopWords", removeWordsUDF($"words"))
  .drop("words")
  .withColumnRenamed("noStopWords", "words")
 
noStopWords.select("words").take(2)

// COMMAND ----------

// MAGIC %md
// MAGIC We can save our work at this point by writing out a parquet file.

// COMMAND ----------

//noStopWords.write.parquet("/mnt/ml-class/smallWords.parquet")

// COMMAND ----------

// MAGIC %md
// MAGIC What is the `DataFrame` doing in the background?

// COMMAND ----------

println(noStopWords.explain(true))

// COMMAND ----------

// MAGIC %md
// MAGIC Let's cache `noStopWords` as we'll use it multiple times shortly.

// COMMAND ----------

noStopWords.cache

// COMMAND ----------

// MAGIC %md
// MAGIC Calculate the number of words in `noStopWords`.  Recall that each row contains an array of words.
// MAGIC  
// MAGIC One strategy would be to take the length of each row and sum the lengths.  To do this use `functions.size`, `functions.sum`, and call `.agg` on the `DataFrame`.
// MAGIC  
// MAGIC Don't forget to refer to the  [Python](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.package) APIs.  For example you'll find detail for the function `size` in the [functions module](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.size) in Python and the [functions package](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) in Scala.

// COMMAND ----------

// MAGIC %md
// MAGIC First, create a `DataFrame` named sized that has a `size` column with the size of each array of words.  Here you can use `func.size`.

// COMMAND ----------

// TODO: Replace <FILL IN> with appropriate code
val sized = noStopWords.<FILL IN>
 
val sizedFirst = sized.select("size").first()
println(sizedFirst)

// COMMAND ----------

sizedFirst(1)

// COMMAND ----------

// TEST
assert(sizedFirst(0) == sizedFirst.getAs[IndexedSeq[String]](1).size, "incorrect implementation for sized")

// COMMAND ----------

// MAGIC %md
// MAGIC Next, you'll need to aggregate the counts.  You can do this using `func.sum` in either a `.select` or `.agg` method call on the `DataFrame`.  Make sure to give your `Column` the alias `numberOfWords`.  There are some examples in [Python](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.GroupedData.agg) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrame) in the APIs.

// COMMAND ----------

// TODO: Replace <FILL IN> with appropriate code
val numberOfWords = sized.<FILL IN>
 
val wordCount = numberOfWords.first()(0)
println(wordCount)

// COMMAND ----------

// TEST
assert(wordCount == 1903220, "incorrect word count")

// COMMAND ----------

// MAGIC %md
// MAGIC Next, we'll compute the word count using `select`, the function `func.explode()`, and then taking a `count()` on the `DataFrame`.  Make sure to name the column returned by the `explode` function 'word'.

// COMMAND ----------

// TODO: Replace <FILL IN> with appropriate code
val wordList = noStopWords.<FILL IN>
 
// Note that we have one word per Row now
wordList.take(3).foreach(println)
val wordListCount = wordList.count()
println(wordListCount)

// COMMAND ----------

assert(wordListCount == 1903220, "incorrect value for wordListCount")

// COMMAND ----------

// MAGIC %md
// MAGIC For your final task, you'll group by word and count the number of times each word occurs.  Make sure to return the counts in descending order and to call them `counts`.
// MAGIC  
// MAGIC For this task, you can use:
// MAGIC  * `DataFrame` operations `groupBy`, `agg`, and `sort`
// MAGIC  * the `Column` operation `alias`
// MAGIC  * functions `func.count` and `func.desc`.

// COMMAND ----------

// TODO: Replace <FILL IN> with appropriate code
val wordGroupCount = wordList
  .<FILL IN>  // group
  .<FILL IN>  // aggregate
  .<FILL IN>  // sort
 
wordGroupCount.take(5).foreach(println)

// COMMAND ----------

// TEST
assert(wordGroupCount.first() == Row("ref", 29263), "incorrect counts.")

// COMMAND ----------

// MAGIC %md
// MAGIC We could also use SQL to accomplish this counting.

// COMMAND ----------

wordList.registerTempTable("wordList")

// COMMAND ----------

val wordGroupCount2 = sqlContext.sql("select word, count(word) as counts from wordList group by word order by counts desc")
wordGroupCount2.take(5).foreach(println)

// COMMAND ----------

// MAGIC %sql
// MAGIC select word, count(word) as counts from wordList group by word order by counts desc

// COMMAND ----------

// MAGIC %md
// MAGIC Finally, let's see how many distinct words we are working with.

// COMMAND ----------

val distinctWords = wordList.distinct
distinctWords.take(5).foreach(println)

// COMMAND ----------

distinctWords.count
