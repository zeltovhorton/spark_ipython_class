// Databricks notebook source exported at Wed, 4 Nov 2015 13:12:12 UTC
// MAGIC %md
// MAGIC # RDD Lab (Scala)
// MAGIC 
// MAGIC In this lab, we'll explore some of the RDD concepts we've discussed. We'll be using a data set consisting of reported crimes in Philadelphia, from the January through October, 2015. The data is from [Open Data Philly](https://www.opendataphilly.org), converted to Parquet. You can find the data at: <https://www.opendataphilly.org/dataset/crime-incidents/resource/edc13da0-8917-41e0-9a63-44f70dd462c2>.
// MAGIC 
// MAGIC We'll use this data to explore some RDD transitions and actions.
// MAGIC 
// MAGIC ## Exercises and Solutions
// MAGIC 
// MAGIC This notebook contains a number of exercises. If, at any point, you're struggling with the solution to an exercise, feel free to look in the **Solutions** notebook (in the same folder as this lab).
// MAGIC 
// MAGIC ## Let's get started.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Load the data
// MAGIC 
// MAGIC The first step is to load the data. Run the following cell to create an RDD containing the data.

// COMMAND ----------

val baseRDD = sc.textFile("dbfs:/mnt/databricks-corp-training/common/philadelphia-crime-data-2015-ytd.csv")

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### Question 1
// MAGIC 
// MAGIC Does the RDD _actually_ contain the data right now?

// COMMAND ----------

// MAGIC %md
// MAGIC ## Explore the data
// MAGIC 
// MAGIC Let's take a look at some of the data.

// COMMAND ----------

baseRDD.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md Okay, there's a header. We'll need to remove that. But we can't just drop the first item. Let's figure out another way to do it.

// COMMAND ----------

val noHeaderRDD = baseRDD.filter { line => ! (line contains "DC_DIST") }

// COMMAND ----------

// MAGIC %md
// MAGIC ### Metadata
// MAGIC 
// MAGIC According to the Open Data Philly site, here's what some of those fields actually mean:
// MAGIC 
// MAGIC Key attribute field names and descriptions
// MAGIC 
// MAGIC * `DC_DIST` (integer): District number
// MAGIC * `SECTOR` (integer): Sector or PSA Number
// MAGIC * `DISPATCH_DATE` (date string): Date of Incident (modified from original data)
// MAGIC * `DISPATCH_TIME` (time string): Time of Incident (modified from original data)
// MAGIC * `DC_KEY`: (text): Unique ID of each crime
// MAGIC * `UCR_General` (integer): Rounded Crime Code
// MAGIC * `TEXT_GENERAL_CODE` (string): Human-readable Crime Code
// MAGIC * `OBJECTID` (integer): Unique row ID
// MAGIC * `POINT_X` (decimal): Latitude where crime occurred
// MAGIC * `POINT_Y` (decimal): Longitude where crime occurred

// COMMAND ----------

// MAGIC %md
// MAGIC ### Question 2
// MAGIC 
// MAGIC Why can't you just do `baseRDD.drop(1)`?

// COMMAND ----------

noHeaderRDD.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Exercise 1
// MAGIC 
// MAGIC Let's make things a little easier to handle, by converting the `noHeaderRDD` to an RDD containing Scala objects.
// MAGIC 
// MAGIC **TO DO**
// MAGIC 
// MAGIC * Split each line into its individual cells.
// MAGIC * Map the RDD into another RDD of appropriate `CrimeData` objects.
// MAGIC * You'll have to decide which fields from the data best map onto the fields of the case class.

// COMMAND ----------

// TAKE NOTE: We are deliberately only some of the fields we need for
// this lab. There's no sense dragging around more data than we need.
case class CrimeData(dateString: String,
                     timeString: String,
                     offense: String,
                     latitude: String,
                     longitude: String)

val dataRDD = noHeaderRDD.map { line =>
  val cols = line.split(",")
  CrimeData(dateString = cols(10), // DISPATCH_DATE
            timeString = cols(11), // DISPATCH_TIME
            offense    = cols(6),  // TEXT_GENERAL_CODE
            latitude   = cols(7),  // POINT_X
            longitude  = cols(8))  // POINT_Y
}
dataRDD.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Exercise 2
// MAGIC 
// MAGIC Next, group the data by type of crime (the `offense` field).

// COMMAND ----------

val groupedByOffenseRDD = dataRDD.groupBy { data => data.offense }

// What does this return? You'll need to know for the next step.
groupedByOffenseRDD.take(10)

// COMMAND ----------

// But here's a better way. Note the use of Scala's "f" string interpolator,
// which is kind of like printf, but with compile-time type safety. For
// more information on the "f" interpolator, see
// http://docs.scala-lang.org/overviews/core/string-interpolation.html

val offenseCounts = dataRDD.map(item => (item.offense, 1)).countByKey()
for ((offense, count) <- offenseCounts) {
  println(f"$offense%30s $count%5d")
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## ETL
// MAGIC There's some junk in our data. Let's clean it up a bit.

// COMMAND ----------

val BadOffenseRE = """^\d+$""".r
val cleanedRDD = dataRDD.map { data =>
  data.copy(offense = data.offense.replaceAll("\"", "").trim())
}.
filter { data =>
  BadOffenseRE.findFirstIn(data.offense).isEmpty
}

// COMMAND ----------

// MAGIC %md
// MAGIC Next, copy the `groupBy` logic, above, but change it to run against the `cleanedRDD`.

// COMMAND ----------

val groupedByOffenseRDD2 = cleanedRDD.groupBy { data => data.offense }

val offenseCounts2 = cleanedRDD.map(item => (item.offense, 1)).countByKey()
for ((offense, count) <- offenseCounts2) {
  println(f"$offense%30s $count%5d")
}

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Exercise 3
// MAGIC Next, create an RDD that counts the number of each offense. How many murders were there in 2013? How many car thefts?

// COMMAND ----------

// But here's a better way. Note the use of Scala's "f" string interpolator,
// which is kind of like printf, but with compile-time type safety. For
// more information on the "f" interpolator, see
// http://docs.scala-lang.org/overviews/core/string-interpolation.html

val offenseCounts = << FILL THIS IN >>
for ((offense, count) <- offenseCounts) {
  println(<< FILL THIS IN >>)
}

// COMMAND ----------

// MAGIC %md ### Question
// MAGIC 
// MAGIC Run the following cell. Can you explain what happened? Is `collectAsMap()` a _transformation_ or an _action_?

// COMMAND ----------

groupedByOffenseRDD2.collectAsMap()

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Exercise 4
// MAGIC 
// MAGIC How many partitions does the base RDD have? What about the `groupedByOffenseRDD2` RDD? How can you find out?
// MAGIC 
// MAGIC **Hint**: Check the [API documentation](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD).

// COMMAND ----------

println(baseRDD.partitions.length)
println(groupedByOffenseRDD2.partitions.length)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Exercise 5
// MAGIC 
// MAGIC Since we're continually playing around with this data, we might as well cache it, to speed things up.
// MAGIC 
// MAGIC **Question**: Which RDD should you cache? 
// MAGIC 
// MAGIC 1. `baseRDD`
// MAGIC 2. `noHeaderRDD`
// MAGIC 3. `dataRDD`
// MAGIC 4. `cleanedRDD`
// MAGIC 5. `groupedByOffenseRDD2`
// MAGIC 6. None of them, because they're all still too big.
// MAGIC 7. It doesn't really matter.

// COMMAND ----------

cleanedRDD.cache()

// COMMAND ----------

cleanedRDD.count()

// COMMAND ----------

// MAGIC %md ### Exercise 6
// MAGIC 
// MAGIC Display the number of homicides.

// COMMAND ----------

val resultRDD1 = cleanedRDD.filter { _.offense.toLowerCase contains "homicide" }.map {item => (item.offense, 1) }.reduceByKey(_ + _)
for ((method, count) <- resultRDD1.collect())
  println(f"$method%10s $count%d")


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ![Stop Sign](http://i.imgur.com/RdABwEB.png)
// MAGIC 
// MAGIC ## Stop Here
// MAGIC 
// MAGIC We'll resume after we've discussed DataFrames.

// COMMAND ----------

// MAGIC %md ### Demonstration
// MAGIC 
// MAGIC Let's plot murders by month. DataFrames are useful for this one.

// COMMAND ----------

// MAGIC %md To do this property, we'll need to parse the dates. That will require knowing their format. A quick sampling of the data will help.

// COMMAND ----------

cleanedRDD.map(_.dateString).take(30)

// COMMAND ----------

// MAGIC %md Okay. We can now parse the strings into actual `Date` objects.
// MAGIC 
// MAGIC **NOTE:** The DataFrame API does _not_ support schemas with `Date` objects in them. We'll need to convert the resulting `Date` to a `java.sql.Timestamp`.

// COMMAND ----------

// MAGIC %md Let's set up the date/time parser.

// COMMAND ----------

import java.text.SimpleDateFormat

val dateFmt = new SimpleDateFormat("yyyy-MM-dd")
val timeFmt = new SimpleDateFormat("HH:mm:ss.S")


// COMMAND ----------

// MAGIC %md Now, we can create the data frame. We'll start with the `no_header_rdd` and map it slightly differently than we did to create `data_rdd`:

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import sqlContext.implicits._
import java.sql.Timestamp

case class CrimeData2(date:      Option[Timestamp],
                      time:      Option[Timestamp],
                      offense:   String,
                      latitude:  Option[BigDecimal],
                      longitude: Option[BigDecimal])
def parseLatLong(s: String) = {
  try {
    Some(BigDecimal(s))
  }
  catch {
    case _: Exception => None
  }
}

def parseDateTime(fmt: SimpleDateFormat, s: String) = {
  try {
    Some(new Timestamp(fmt.parse(s).getTime))
  }
  catch {
    case _: Exception => None
  }
}

val df = noHeaderRDD.map { line =>
  val cols = line.split(",")
  val timeString = cols(11)
  val dateString = cols(10)
  CrimeData2(parseDateTime(dateFmt, dateString),
             parseDateTime(timeFmt, timeString),
             cols(6), 
             parseLatLong(cols(7)),
             parseLatLong(cols(8)))
}.
map { data =>
  data.copy(offense = data.offense.replaceAll("\"", "").trim())
}.
filter { data =>
  BadOffenseRE.findFirstIn(data.offense).isEmpty
}.
toDF


// COMMAND ----------

df.printSchema()

// COMMAND ----------

import org.apache.spark.sql.functions._
display( 
  df.filter(lower($"offense") like "%homicide%")
    .select(month($"date").as("month"), $"offense")
    .groupBy($"month").count()
)

// COMMAND ----------

// MAGIC %md What about all crimes per month?

// COMMAND ----------

display( df.select(month($"date").as("month")).groupBy("month").count() )

// COMMAND ----------

// MAGIC %md We can also plot the frequency of crimes by hour of day.

// COMMAND ----------

display(df.select(hour($"time").as("hour"), $"offense").groupBy($"hour").count())

// COMMAND ----------


