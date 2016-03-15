// Databricks notebook source exported at Thu, 5 Nov 2015 19:30:06 UTC
// MAGIC %md
// MAGIC # Spark Streaming and Wikipedia ![Wikipedia Logo](http://sameerf-dbc-labs.s3-website-us-west-2.amazonaws.com/data/wikipedia/images/w_logo_for_labs.png)
// MAGIC 
// MAGIC ### Time to complete: 20-30 minutes
// MAGIC 
// MAGIC #### Business Questions:
// MAGIC 
// MAGIC * Question 1: Where are recent anonymous English Wikipedia editors located in the world?
// MAGIC * Question 2: Are edits being made to the English Wikipedia from non-English speaking countries? If so, where?
// MAGIC 
// MAGIC 
// MAGIC #### Technical Accomplishments:
// MAGIC 
// MAGIC * Learn how to tweak Spark Streaming configuration values to affect how much data ends up in the RDDs Streaming creates.
// MAGIC * Increased understanding of the Spark Streaming UI.
// MAGIC * How to visualize data using both the Databricks notebook and an external JavaScript graphing API.
// MAGIC * Understand to how recognize and avoid a potentially expensive Cartesian product on a DataFrame JOIN operation.

// COMMAND ----------

// MAGIC %md
// MAGIC In this lab, we're going to analyze some streaming Wikipedia edit data.
// MAGIC 
// MAGIC First, let's get some imports out of the way.

// COMMAND ----------

import com.databricks.training.helpers.Enrichments.EnrichedArray
import com.databricks.training.helpers.TimeConverters._
import com.databricks.training.helpers.TimeHelpers._
import com.databricks.training.helpers.InetHelpers._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming._
import scala.util.Random

// COMMAND ----------

// MAGIC %md Now, some configuration constants. **DON'T EDIT THESE.**

// COMMAND ----------

val ID                   = Random.nextInt(1000000)
val WorkingDirectory     = s"dbfs:/tmp/streaming/$ID"
val CheckpointDirectory  = s"$WorkingDirectory/checkpoint"
val ParquetOutput        = s"$WorkingDirectory/out.parquet"
val StreamingServerHost  = "52.89.53.194"
val StreamingServerPort  = 9002

// COMMAND ----------

// MAGIC %md Here's the configuration section you **can** edit. Recall that the batch interval defines how long Spark Streaming will buffer data before creating an RDD. We typically choose a batch interval between 500 milliseconds and 2 seconds. However, Wikipedia edits come in relatively slowly, so a slightly longer batch interval is warranted.

// COMMAND ----------

val BatchInterval = Seconds(5)

// COMMAND ----------

dbutils.fs.rm(WorkingDirectory, recurse=true)
dbutils.fs.mkdirs(CheckpointDirectory)


// COMMAND ----------

// MAGIC %md
// MAGIC ## The Streaming Logic
// MAGIC 
// MAGIC The following object contains our Spark Streaming logic. Within a notebook (and, sometimes, the Spark Shell), it often helps to isolate your streaming logic inside a single Scala object, to keep the Spark scope analyzer from pulling in unnecessary objects from the notebook scope.
// MAGIC 
// MAGIC The Streaming code:
// MAGIC 
// MAGIC * Receives each Streaming-produced RDD, which contains individual change records as JSON objects (strings)
// MAGIC * Converts the RDD of string JSON records into an RDD of Scala `WikipediaChange` objects
// MAGIC * Filters out non-article edits (e.g., changes to someone's Wikipedia profile page).
// MAGIC * Saves the changes to a Parquet file we can query later
// MAGIC 
// MAGIC ### The `WikipediaChange` object
// MAGIC 
// MAGIC For convenience and readability, the `WikipediaChange` class (a Scala `case class`) is defined in a separate library. It consists of the following fields:
// MAGIC 
// MAGIC * `anonymous` (`Option[Boolean]`): Whether or not the change was made by an anonymous user.
// MAGIC * `channel` (`Option[String]`): The Wikipedia IRC channel, e.g., "#en.wikipedia"
// MAGIC * `comment` (`Option[String]`): The comment associated with the change (i.e., the commit message).
// MAGIC * `delta` (`Option[Int]`): The number of lines changes, deleted, and/or added.
// MAGIC * `flag` (`Option[String]`): A flag indicating the kind of change.
// MAGIC * `namespace` (`Option[String]`): The page's namespace. See <https://en.wikipedia.org/wiki/Wikipedia:Namespace>
// MAGIC * `newPage` (`Option[Boolean]`): Whether or not the edit created a new page.
// MAGIC * `page`: (`Option[String]`): The printable name of the page that was edited
// MAGIC * `pageUrl` (`Option[String]`): The URL of the page that was edited.
// MAGIC * `robot` (`Option[Boolean]`): Whether the edit was made by a robot (`true`) or a human (`false`).
// MAGIC * `timestamp` (`Option[Timestamp]`): The time the edit occurred, as a `java.sql.Timestamp`.
// MAGIC * `unpatrolled` (`Option[Boolean]`): Whether or not the article is patrolled. See <https://en.wikipedia.org/wiki/Wikipedia:New_pages_patrol/Unpatrolled_articles>
// MAGIC * `url` (`Option[String]`): The URL of the edit diff.
// MAGIC * `user` (`Option[String]`): The user who made the edit or, if the edit is anonymous, the IP address associated with the anonymous editor.
// MAGIC * `userUrl` (`Option[String]`): The Wikipedia profile page of the user, if the edit is not anonymous.
// MAGIC * `wikipedia` (`Option[String]`): The readable name of the Wikipedia that was edited (e.g., "English Wikipedia").
// MAGIC * `wikipediaLong` (`Option[String]`): The long readable name of the Wikipedia that was edited. Might be the same value as the `wikipedia` field.
// MAGIC * `wikipediaShort` (`Option[String]`): The short name of the Wikipedia that was edited (e.g., "en").
// MAGIC * `wikipediaUrl` (`Option[String]`): The URL of the Wikipedia edition containing the article.
// MAGIC 
// MAGIC Note that all fields are Scala `Option` types, which indicates that they may or may not actually be present.

// COMMAND ----------

object Runner extends Serializable {
  import java.sql.Timestamp
  import org.apache.spark.sql._
  import com.databricks.training.wikipedia.{WikipediaChange, WikipediaChangeParser}
  
  private val SpecialPagesRE = """wiki/[A-Z]\w+:""".r
  
  /** Convenience function to stop any active streaming context running
    * in the current JVM. Also cleans out the checkpoint directory.
    */
  def stop(): Unit = {
    StreamingContext.getActive.map { ssc =>
      ssc.stop(stopSparkContext=false)
      println("Stopped running streaming context.")
    }
    
    // Remove the checkpoint directory, just to be safe.
    dbutils.fs.rm(CheckpointDirectory, recurse=true)
  }
  
  /** Convenience function to start our stream.
    */
  def start() = {
    StreamingContext.getActiveOrCreate(checkpointPath = CheckpointDirectory,
                                       creatingFunc   = createContext _)
  }
  
  /** Convenience function to stop any running context and start a new one.
    */
  def restart() = {
    stop()
    start()
  }
  
  private def createContext(): StreamingContext = {
    val ssc = new StreamingContext(sc, BatchInterval)
    ssc.checkpoint(CheckpointDirectory)
    println(s"Connecting to $StreamingServerHost, port $StreamingServerPort")
    val baseDS = ssc.socketTextStream(StreamingServerHost, StreamingServerPort)

    // First, parse the JSON data (which comes in one JSON object per line) into
    // something more usable.
    //
    // We use flatMap here because WikipediaChangeParser.parseChange() returns
    // an Option. If it returns a None, meaning "unparseable record", flatMap()
    // will filter it out. If it returns Some, flatMap() will unpack the Some.
    baseDS.flatMap { jsonString =>
      // Convert the JSON to a WikipediaChange object.
      WikipediaChangeParser.parseChange(jsonString)
    }.
    filter { change =>
      // Filter out things like "wiki/Talk:Topic", "wiki/User:someuser", "wiki/Special:Log", etc., by
      // getting rid of anything that has wiki/<uppercase><wordchars>:
      change.pageUrl.map { url => ! SpecialPagesRE.findFirstIn(url).isDefined }.
                     getOrElse(false)
    }.
    filter { edit =>
      // If the edit is anonymous, only keep it if the IP address is parseable.
      // If it's not anonymous, keep it unconditionally.
      val anon = edit.anonymous.getOrElse(false)
      if (! anon) {
        // Non-anonymous entry. Keep.
        true
      }
      else {
        // The user field could be empty. If it isn't, try to parse it
        // as an IP address. If that fails, discard this record.
        val ip = edit.user.getOrElse("")
        parseIPAddress(ip).map(_ => true).getOrElse(false)
      }
    }.
    foreachRDD { rdd =>
      // Append to the Parquet file.
      val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
      if (! rdd.isEmpty) {
        // Pare it down to one partition, to make the Parquet file easier
        // to read.
        val df = sqlContext.createDataFrame(rdd.repartition(1))
        df.write.mode(SaveMode.Append).parquet(ParquetOutput)
      }
    }

    ssc.start()
    ssc
  }
}

// COMMAND ----------

// Test: Ensure that the Runner object is serializable.
(new java.io.ObjectOutputStream(new java.io.ByteArrayOutputStream)).writeObject(Runner)

// COMMAND ----------

Runner.restart()

// COMMAND ----------

// MAGIC %md The following cell uses a Databricks-specific library to determine whether the Parquet file is there yet.

// COMMAND ----------

display( dbutils.fs.ls(ParquetOutput) )

// COMMAND ----------

// MAGIC %md
// MAGIC We're buffering incoming RDDs into a persistent Parquet file. Let's load it up and see what it contains. 
// MAGIC 
// MAGIC **NOTE**: It's possible for the read to fail, if you happen to catch the Parquet file in the middle of a write operation (or if the file doesn't exist yet). If that happens, just try again. 

// COMMAND ----------

val df = sqlContext.read.parquet(ParquetOutput)
println(s"Edits so far: ${df.count}\n")

// COMMAND ----------

// MAGIC %md Let the stream run for a few minutes, then run the following cell to stop the stream. We're buffering to a Parquet file, and, by default, Spark caches Parquet file metadata. Thus, unless we reload the DataFrame periodically, we don't "see" new data in the Parquet file. So, from this point onward, we'll be doing batch processing of the Parquet file.

// COMMAND ----------

Runner.stop()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Repartitioning
// MAGIC 
// MAGIC It's likely that we have a lot of small partitions, because each RDD is relatively small and its partitions are written to a separate Parquet `part` files. So, we end up with a Parquet file with lots of `part` files each of which is mapped to an RDD partition when read and, each of which contains a relatively small number of edits. Let's take a look.

// COMMAND ----------

val info = df.rdd.glom().
              mapPartitionsWithIndex { (index, iterator) => iterator.map(v => (index, v)) }.
              map { case (key, values) => (key, values.size) }.
              collect().
              sortWith { _._2 > _._2 }

val totalItems = info.map(_._2).sum
println(s"Total partitions: ${info.length}")
println(s"Total of all counts: $totalItems")

info.sortWith(_._1 < _._1).foreach { case (partitionIndex, count) =>
  println(f"Partition $partitionIndex%3d $count%d")
}


// COMMAND ----------

// MAGIC %md
// MAGIC ### Question 1
// MAGIC 
// MAGIC In the streaming logic (up in the `Runner` object), we only process an RDD if it's not empty. Note the `if` statement:
// MAGIC 
// MAGIC ```
// MAGIC if (! rdd.isEmpty) {
// MAGIC   // write to Parquet
// MAGIC }
// MAGIC ```
// MAGIC 
// MAGIC Is it possible, then, that we could end up with Parquet `part` files that contain no data? If so, why?

// COMMAND ----------

// MAGIC %md Many partitions with very few data items (sometimes no data items) in each one. Let's reduce the number of partitions so that we have at least several items per partition.

// COMMAND ----------

val df2 = df.repartition(4)
println(s"df2: number of partitions = ${df2.rdd.partitions.length}\n")

// COMMAND ----------

// MAGIC %md ### Caching
// MAGIC 
// MAGIC Let's cache this second DataFrame, as well, to speed up our subsequent analyses.

// COMMAND ----------

df2.cache()
df2.count()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Question 2
// MAGIC How might you increase the number of edit records in each partition of the Parquet file? What configuration item or items might help here?

// COMMAND ----------

// MAGIC %md
// MAGIC ## A Bit of Visualization

// COMMAND ----------

// MAGIC %md
// MAGIC How many anonymous edits vs. authenticated edits are there? Let's visualize it.

// COMMAND ----------

display(df2.groupBy($"anonymous").count())

// COMMAND ----------

// MAGIC %md We can also visualize it using an external graphic library, such as [Google Charts](https://developers.google.com/chart/). Within Databricks notebooks, we have to "escape" to HTML, using a special `displayHTML()` notebook function. In the example, below, we're using the [Gauge](https://developers.google.com/chart/interactive/docs/gallery/gauge) chart. We'll build up the HTML in a separate cell, so that the graph won't be cluttered up with code.
// MAGIC 
// MAGIC **Step 1**: Divide the data into "anonymous" and "authenticated" groups. We'll use the DataFrame `groupBy()` function, and we'll pull the results back into a Scala `Map`.

// COMMAND ----------

val counts = df2.groupBy($"anonymous").
                 count().
                 collect().
                 map { row => (row(0).asInstanceOf[Boolean], row(1).asInstanceOf[Long])}.
                 toMap

// COMMAND ----------

// MAGIC %md **Step 2**: Create the HTML. (This logic could easily be factored into a library.)

// COMMAND ----------

val graphHeight = 200
val graphWidth  = 500
val maxValue    = counts.values.sum

val html = s"""
<!DOCTYPE 
  <head>
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <script type="text/javascript">
      google.load("visualization", "1", {packages:["gauge"]});
      google.setOnLoadCallback(drawChart);
      function drawChart() {

        var data = google.visualization.arrayToDataTable([
          ['Label', 'Value'],
          ['Anonymous', ${counts(true)}],
          ['Logged in', ${counts(false)}]
         ]);

        var max = $maxValue + 20;
        var redStart = max - 50;
        var yellowStart = redStart - 50;
        var options = {
          max:         max,
          width:       $graphWidth,
          height:      $graphHeight,
          redFrom:     redStart, 
          redTo:       max,
          yellowFrom:  yellowStart, 
          yellowTo:    redStart - 1,
          minorTicks:  5
        };

        var chart = new google.visualization.Gauge(document.getElementById('chart_div'));
        chart.draw(data, options);
      }
    </script>
  </head>
  <body>
    <div id="chart_div" style="width: ${graphWidth}px; height: ${graphHeight}px;"></div>
  </body>
</html>
"""

// COMMAND ----------

// MAGIC %md **Step 3**: Visualize!

// COMMAND ----------

displayHTML(html)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Exercise 1 (OPTIONAL: Intermediate)
// MAGIC 
// MAGIC Using the Databricks Pie Chart, graph anonymous vs. authenticated vs. robot edits, so you can visualize the percentage of edits made by robots, by authenticated users, and by anonymous users. The `robot` field in the `WikipediaChange` class defines whether the edit was made by a robot or not.
// MAGIC 
// MAGIC **HINT**: The DataFrame `when()` method is useful here.
// MAGIC 
// MAGIC **NOTE**: If this exercise seems too difficult, feel free to skip it. The solution is in the accompanying "Solutions (Scala)" notebook.

// COMMAND ----------

display(<<< FILL IN >>>)



// COMMAND ----------

// MAGIC %md
// MAGIC ## Anonymous Edits
// MAGIC 
// MAGIC Let's do some analysis of just the anonymous editors. To make our job a little easier, we'll create another DataFrame containing _just_ the anonymous users.

// COMMAND ----------

val anonDF = df2.filter($"anonymous" === true)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### Exercise 2
// MAGIC 
// MAGIC Wikipedia anonymous edits record the editor's IP address in the `user` field, rather than the authenticated user name. Run a query that counts the number of instances of each IP address, displaying each IP address and the number of edits associated with it.

// COMMAND ----------

display( anonDF. <<FILL THIS IN>> )

// COMMAND ----------

// MAGIC %md
// MAGIC ### Visualizing Anonymous Editors' Locations
// MAGIC 
// MAGIC ![World Map](http://i.imgur.com/66KVZZ3.png)
// MAGIC 
// MAGIC Let's see if we can geocode the IP addresses to see where each editor is in the world. As a bonus, we'll plot the geocoded locations on a world map.
// MAGIC 
// MAGIC We'll need a data set that maps IP addresses to their locations in the world. Fortunately, we have one already prepared, built from from the [IP2Location DB5-Lite](http://lite.ip2location.com/databases) database augmented with ISO 3-letter country codes from the [ISO](http://iso.org) web site.

// COMMAND ----------

val dfIP = sqlContext.read.parquet("dbfs:/mnt/databricks-corp-training/common/ip-geocode.parquet")

// COMMAND ----------

// MAGIC %md
// MAGIC When comparing IP addresses, it helps to convert them to numeric values. A user-defined function (UDF) will help there. 
// MAGIC 
// MAGIC This UDF uses a library of IP address helpers, already uploaded to each cluster. The following import, at the top of the notebook, pulls this library into scope:
// MAGIC 
// MAGIC ```
// MAGIC import com.databricks.training.helpers.InetHelpers._
// MAGIC ```
// MAGIC 
// MAGIC Note that `ipToNumber` returns a `BigInt`, which isn't supported by DataFrames. So, we convert it `BigDecimal`, which _is_ supported.
// MAGIC 
// MAGIC **_However_**...
// MAGIC 
// MAGIC Defining the UDF the "normal" way causes problems, as outlined in detail in the accompanying "UDF-Problems" notebook. We'll define our UDF as a SQL-only UDF.

// COMMAND ----------

import org.apache.spark.sql.api.java.UDF1

object ParseIPToNumber extends UDF1[String, Option[BigDecimal]] {
  def call(inputValue: String) = parseIPAddress(inputValue).map(inetAddr => BigDecimal(ipToNumber(inetAddr)))
}
sqlContext.udf.register("IP2Number", ParseIPToNumber, DecimalType(38, 0))

// COMMAND ----------

df2.registerTempTable("df2")
val df3 = sqlContext.sql("SELECT user AS ipAddress, IP2Number(user) AS ipAddressNum FROM df2 WHERE user LIKE '%:%'")
df3.show()

// COMMAND ----------

// MAGIC %md
// MAGIC **_Finally_**, we have a DataFrame that contains the anonymous IP addresses as numbers. We can use that information to geocode the IP addresses.
// MAGIC 
// MAGIC We have a problem, however: Joining the edits directly against the IP DataFrame could be expensive. Let's see why: We'll create just such a join and examine the query plan.

// COMMAND ----------

val r = df3.join(dfIP, df3("ipAddressNum") >= dfIP("startingIP") && df3("ipAddressNum") <= dfIP("endingIP")).
            select($"ipAddress", $"country", $"stateProvince", $"city")


// COMMAND ----------

// MAGIC %md
// MAGIC Note the `CartesianProduct` in the physical plan:

// COMMAND ----------

r.explain()

// COMMAND ----------

// MAGIC %md Depending on the size of the input sets, that Cartesian product can be expensive. The edits, themselves, are a small data set. However, that wouldn't be the case if we let the stream run for awhile. And, the `dfIP` data set isn't tiny: It has more than 7 million rows.
// MAGIC 
// MAGIC So, rather than use a traditional join, we're going to load the IP address geocoding data into memory, store it in a broadcast variable, and do manual lookups against the `ipLookup` broadcast variable, using a binary search.
// MAGIC 
// MAGIC **NOTE:** 
// MAGIC 
// MAGIC * We're using a `GeoIPEntry` class (a case class, really) that's defined in a library.
// MAGIC * The binary search function, `geocodeIP()` is also defined in the same library?in fact, several variants of it.
// MAGIC * Loading the geo-IP data and creating the broadcast variable can take a couple minutes. Please be patient.
// MAGIC * We mark the `ipTable` variable as transient (`@transient`) to prevent it from being serialized accidentally by some of the closures. (This can be a problem in notebooks.)

// COMMAND ----------

import com.databricks.training.helpers.InetGeocode.GeoIPEntry

@transient val ipTable = dfIP.orderBy($"endingIP").collect().map { row =>
  GeoIPEntry(startingIP     = row(0).toString,
             endingIP       = row(1).toString,
             countryCode2   = row(2).toString,
             countryCode3   = row(3).toString,
             country        = row(4).toString,
             stateProvince  = row(5).toString,
             city           = row(6).toString,
             latitude       = row(7).asInstanceOf[Double].toFloat,
             longitude      = row(8).asInstanceOf[Double].toFloat)
}

// COMMAND ----------

println(s"Created broadcast variable from ${ipTable.length} IPv4 and IPv6 address ranges.")
val ipLookup = sc.broadcast(ipTable)


// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a quick look at the geocoding function, with some test IP addresses, including one RFC-1918 ("for internal use only") IP address. All but the "172.16" address should produce a geolocated result.

// COMMAND ----------

import com.databricks.training.helpers.InetGeocode.InetGeocoder

val geocoder = new InetGeocoder(ipLookup.value)
for (ip <- Vector("69.164.215.38", "66.249.9.74", "192.241.240.249", "172.16.87.2" /* invalid */, "2601:89:4301:95BE:5570:C9AE:1A50:3C09")) {
  geocoder.geocodeIP(ip).map { g => 
    println(s"IP $ip: $g")
  }.
  getOrElse {
    println(s"IP $ip: NO MATCH")
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC Okay, _now_ we need a way to geocode the IP addresses in our DataFrame. Once again, a UDF solves the problem:

// COMMAND ----------

val uGeocodeIP = sqlContext.udf.register("uGeocodeIP", (ipNum: java.math.BigDecimal) => {
  val g = new InetGeocoder(ipLookup.value)
  g.geocodeIP(BigDecimal(ipNum))
})

// COMMAND ----------

// MAGIC %md
// MAGIC _Now_ we can use the UDF to geocode the IP addresses efficiently. We only need one instance of each IP address.

// COMMAND ----------

val geocoded = df3.select($"ipAddressNum").groupBy($"ipAddressNum").count().select($"ipAddressNum", $"count", uGeocodeIP($"ipAddressNum").as("geocoded"))
geocoded.show()

// COMMAND ----------

// MAGIC %md Note that we added a new `geocoded` column, and it's a _structural_ (or nested) data type. Let's take a closer look at the data, to get a sense for where the anonymous editors are. We don't need all the geocoded columns.

// COMMAND ----------

display(geocoded.select($"geocoded.countryCode3", $"geocoded.country", $"geocoded.stateProvince", $"geocoded.city", $"count").orderBy($"count".desc))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Visualizing the same data on a world map gives us a better sense of the data. Fortunately, the Databricks notebooks support a World Map graph. This graph type requires the 3-letter country codes and the counts, so that's all we're going to extract.

// COMMAND ----------

val geocoded2 = geocoded.select($"count", $"geocoded.countryCode3")

// COMMAND ----------

display(geocoded2)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Exercise 3
// MAGIC 
// MAGIC Remove the United States, Canada and the UK from the data and re-plot the results, to see who's editing English Wikipedia entries from countries where English is not the primary language.
// MAGIC 
// MAGIC **HINT**: Use the `display()` function. You'll need to arrange the Plot Options appropriately. Examine the Plot Options for the previous map to figure out what to do.

// COMMAND ----------

display( <<<FILL IN>>> )

// COMMAND ----------


