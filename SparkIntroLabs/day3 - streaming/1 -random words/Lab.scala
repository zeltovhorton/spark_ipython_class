// Databricks notebook source exported at Thu, 5 Nov 2015 17:01:31 UTC
// MAGIC %md 
// MAGIC # Spark Streaming Lab (Scala)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Setup
// MAGIC 
// MAGIC This first section is largely just setup. If you want to play around with the configuration constants, feel free to do so. But leave the imports and the `require` alone.

// COMMAND ----------

// Necessary imports and other constants. DON'T change these.

import com.databricks.training.RandomWordSource
import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.sql._

val WordsFile = "dbfs:/mnt/training/word-game-dict.txt"

val rng = new java.security.SecureRandom
val randomID = rng.nextInt(10000)

// NOTE: This name has to be unique per-user, hence the random suffix.
val OutputDir   = "dbfs:/tmp/words"
val ParquetFile = s"$OutputDir$randomID"

// Temporary table name. Shouldn't clash with anyone else, unless multiple
// people are using the same cluster. Just in case, we'll use the random ID.
val TableName = s"words_$randomID"

// Ensure that we're running against Spark 1.4.0 or better.
require(sc.version.replace(".", "").toInt >= 140, "Spark 1.4.0 or greater is required to run this notebook. Please attach notebook to a 1.4.x cluster.")

dbutils.fs.mkdirs(OutputDir)


// COMMAND ----------


// --------------------------------------------------------------------
// CONFIGURATION
// --------------------------------------------------------------------

val WordsPerCycle   = 10000  // A "cycle" is just an internal loop within the StreamingWordSource. It's not 
                             // Spark-related at all.
val InterCycleSleep = 500    // number of milliseconds to wait between each burst of words

val BatchIntervalSeconds = 1

val CheckpointDirectory = Some(s"$OutputDir/checkpoint/$randomID")

// RememberSeconds defines the number of seconds that each DStream in the context
// should remember RDDs it generated in the last given duration. A DStream remembers
// an RDD only for a limited duration of time, after which it releases the RDD for
// garbage collection. You can change the duration (which is useful, if you wish to
// query old data outside the DStream computation).
val RememberSeconds = Some(60)

// END CONFIGURATION

// COMMAND ----------

// MAGIC %md
// MAGIC ## A helper function to create our streaming source of words

// COMMAND ----------

// Load the dictionary.
val rdd = sc.textFile(WordsFile)
val words = rdd.collect()

/** Create and return a Streaming source we can use.
  */
def createRandomWordSource() = {
  // Create a stream from our RandomWordSource
  new RandomWordSource(dictionary      = words,
                       wordsPerCycle   = WordsPerCycle,
                       interCycleSleep = InterCycleSleep)
}

// COMMAND ----------

rdd.count

// COMMAND ----------

// MAGIC %md
// MAGIC ## Create our Streaming Process
// MAGIC 
// MAGIC The code in the next cell is the meat of our Spark Streaming application. It:
// MAGIC 
// MAGIC * creates a Streaming source that randomly produces words from the dictionary
// MAGIC * creates a stream to read from the source
// MAGIC * configures the stream so that each RDD the stream produces is:
// MAGIC     * mapped to another RDD that counts the words, which is then
// MAGIC     * persisted to a Parquet file.
// MAGIC     
// MAGIC **NOTE** In Databricks (and in a Scala REPL), it helps to wrap your streaming solution in an object, to prevent scope confusion when Spark attempts to gather up and serialize the variables and functions for distribution across the cluster.

// COMMAND ----------

object Runner extends Serializable {
  
  case class WordCount(word: String, count: Int)

  def stop(): Unit = {
    StreamingContext.getActive.map { ssc =>
      ssc.stop(stopSparkContext = false)
      println("Stopped Streaming Context.")
    }
  }
  
  def start(): Unit = {
    // Create the streaming context.
    val ssc = if (CheckpointDirectory.isDefined) { 
      val dir = CheckpointDirectory.get
      StreamingContext.getActiveOrCreate(dir, createStreamingContext _)
    }
    else {
      StreamingContext.getActiveOrCreate(createStreamingContext _)
    }

    // Start our streaming context.
    ssc.start()

    println("Started/rejoined streaming context")
    
    // Wait for it to terminate (which it won't, because the source never stops).
    //ssc.awaitTerminationOrTimeout(BatchIntervalSeconds * 5 * 1000)
  }

  def restart(): Unit = {
    stop()
    start()
  }
  
  private def createStreamingContext(): StreamingContext = {
  
    // Create a StreamingContext
    val ssc = new StreamingContext(sc, Seconds(BatchIntervalSeconds))
  
    // For saving checkpoint info so that it can recover from failed clusters
    if (CheckpointDirectory.isDefined) ssc.checkpoint(CheckpointDirectory.get)
  
    val stream = ssc.receiverStream(createRandomWordSource())
    
    // What we're getting is a stream of words. As each RDD is created by
    // Spark Streaming, have the stream create a new RDD that counts the
    // words.
    val wordCountStream = stream.map { word => (word, 1) }.
                                 reduceByKey(_ + _).
                                 map { case (word, count) => WordCount(word, count) }

    // Save each RDD in our Parquet table. We'll likely end up with multiple rows for each word, but
    // we can sum those up easily enough.

    wordCountStream.foreachRDD { rdd => 
      val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
      val df = sqlContext.createDataFrame(rdd)
      df.write.mode(SaveMode.Append).parquet(ParquetFile)
    }
    
    ssc
  }    
}

// COMMAND ----------

// MAGIC %md Verify that our runner can be serialized.

// COMMAND ----------

val out = new java.io.ObjectOutputStream(new java.io.ByteArrayOutputStream)
out.writeObject(Runner)

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Start it all up
// MAGIC 
// MAGIC Here, we start the stream and wait until everything finishes. In this case, it won't finish on its own; it'll continue until we shut it down.

// COMMAND ----------

Runner.restart()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Query our Parquet file
// MAGIC 
// MAGIC The cell, below, loads and queries the Parquet table that's being built by the stream. Run that cell repeatedly, and watch how the data changes. You should see multiple rows for the same word, after awhile.
// MAGIC 
// MAGIC **Question**: Why is that happening?
// MAGIC 
// MAGIC **NOTE**: Because of timing issues, the query might occasionally fail with an obscure-looking error. Just reissue it if that happens.

// COMMAND ----------

val df = sqlContext.read.parquet(ParquetFile)
display(df.orderBy($"count".desc))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Shut it down
// MAGIC 
// MAGIC Wait about five minutes, then stop the stream.

// COMMAND ----------

Runner.stop()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Aggregating the counts
// MAGIC 
// MAGIC Let's aggregate the counts. SQL is as handy a way as any.

// COMMAND ----------

df.registerTempTable(TableName)
display(sqlContext.sql(s"SELECT word, sum(count) AS total FROM $TableName GROUP BY word ORDER BY total DESC, word LIMIT 50"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Assignment
// MAGIC 
// MAGIC Modify the above `Runner` to filter the stream so that it:
// MAGIC 
// MAGIC * discards words more than 7 characters long,
// MAGIC * discards words that have a Scrabble base score less than 15,
// MAGIC * saves the remaining words and point values in a new Parquet table
// MAGIC * queries that table for the 10 top-scoring words in the stream
// MAGIC 
// MAGIC A base score is the score of a word in Scrabble _before_ accounting for any multipliers related to board placement (e.g., before applying triple-word scores, double-letter scores, and the like).
// MAGIC 
// MAGIC This assignment is a variation of the example above. You can use DataFrame operations or SQL to accomplish the work. You can also re-use `wordsSource`, from above; there's no need to recreate it.
// MAGIC 
// MAGIC (**NOTE**: This is an imperfect problem statement, because it doesn't take letter frequencies into account. There's only one "Z" in a set of Scrabble tiles, but a solution that conforms to the above description will assign 10 points to _every_ "Z" in a word that has multiple "Z" letters?even though that's impossible in Scrabble. For our purposes, that's fine.)
// MAGIC 
// MAGIC **The following cell defines the outline of the solution. Your solution code goes at the bottom of the `run()` method.**

// COMMAND ----------

val SolutionParquet = s"$OutputDir/scrabble_$randomID"
val SolutionTable   = s"scrabble_$randomID"

object Solution extends Serializable {
 
  case class WordScore(word: String, score: Int)
  
  def stop(): Unit = {
    StreamingContext.getActive.map { ssc =>
      ssc.stop(stopSparkContext = false)
      println("Stopped Streaming Context.")
    }
  }
  
  def start(): Unit = {
    // Create the streaming context.
    val ssc = CheckpointDirectory.map { checkpointDir =>
      StreamingContext.getActiveOrCreate(checkpointDir, createStreamingContext _)
    }.
    getOrElse {
      StreamingContext.getActiveOrCreate(createStreamingContext _)
    }

    // Start our streaming context.
    ssc.start()

    println("Started/rejoined streaming context")
    
    // Wait for it to terminate (which it won't, because the source never stops).
    //ssc.awaitTerminationOrTimeout(BatchIntervalSeconds * 5 * 1000)
  }

  def restart(): Unit = {
    stop()
    start()
  }
  
  private def createStreamingContext(): StreamingContext = {
    // Use this map to calculate the point value of each word, letter by letter.
    val LetterPoints = Map('a' ->  1, 'b' ->  3, 'c' ->  3, 'd' ->  2, 'e' ->  1,
                           'f' ->  4, 'g' ->  2, 'h' ->  4, 'i' ->  1, 'j' ->  8,
                           'k' ->  5, 'l' ->  1, 'm' ->  3, 'n' ->  1, 'o' ->  1,
                           'p' ->  3, 'q' -> 10, 'r' ->  1, 's' ->  1, 't' ->  1,
                           'u' ->  2, 'v' ->  4, 'w' ->  4, 'x' ->  8, 'y' ->  4,
                           'z' -> 10)

    // Create the streaming context.
    val ssc = new StreamingContext(sc, Seconds(BatchIntervalSeconds))
    
    // ****** PUT YOUR SOLUTION HERE

    // ******
    
    ssc
  }
}

// COMMAND ----------

Solution.restart()

// COMMAND ----------

val df = sqlContext.read.parquet(SolutionParquet)
df.registerTempTable(SolutionTable)
display(sqlContext.sql(s"SELECT word, score, count(score) AS total FROM $SolutionTable GROUP BY word, score ORDER BY score DESC, total DESC, word LIMIT 50"))

// COMMAND ----------

Solution.stop()

// COMMAND ----------


