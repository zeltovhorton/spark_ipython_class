// Databricks notebook source exported at Thu, 5 Nov 2015 19:27:31 UTC
// MAGIC %md
// MAGIC # Log File Streaming Lab
// MAGIC 
// MAGIC In this lab, we'll be reading (fake) log data from a stream.

// COMMAND ----------

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql._
import scala.util.Random
import java.sql.Timestamp

require(sc.version.replace(".", "").toInt >= 140, "Spark 1.4.0 or greater is required.")

// COMMAND ----------

val BatchIntervalSeconds= 1
val OutputDir = s"dbfs:/tmp/streaming/logs/${Random.nextInt(1000)}"
val OutputFile = s"$OutputDir/${(new java.util.Date).getTime}.parquet"
val CheckpointDir = s"$OutputDir/checkpoint"


// COMMAND ----------

// MAGIC %md
// MAGIC We need to pull in some configuration data.

// COMMAND ----------

// MAGIC %run "/:Labs/streaming/2-Log-Server/Scala/Configuration"

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Create our Streaming Process
// MAGIC 
// MAGIC We're going to connect to a TCP server that serves log messages, one per line. We'll read the messages and write them to a Parquet file.
// MAGIC 
// MAGIC We're going to be using a complicated-looking regular expression to take each log message apart:
// MAGIC 
// MAGIC ```
// MAGIC ^\[(.*)\]\s+\(([^)]+)\)\s+(.*)$
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC The following object contains the code for our stream reader.
// MAGIC 
// MAGIC **NOTE**: In Databricks (and in a Scala REPL), it helps to wrap your streaming solution in an object, to prevent scope confusion when Spark attempts to gather up and serialize the variables and functions for distribution across the cluster.

// COMMAND ----------

object Runner extends Serializable {
  import java.text.SimpleDateFormat
  import java.util.Date
  import java.sql.Timestamp
  
  case class LogMessage(messageType: String, timestamp: Timestamp, message: String)

  // Log messages look like this: [2015/09/10 21:15:46.680] (ERROR) Server log backup FAILED. See backup logs.
  private val LogLinePattern = """^\[(.*)\]\s+\(([^)]+)\)\s+(.*)$""".r
  
  /** Stop the streaming context.
    */
  def stop() {
    StreamingContext.getActive.map { ssc =>
      ssc.stop(stopSparkContext=false, stopGracefully=false)
      println("Stopped active streaming context.")
    }
  }
  
  /** Start the streaming context.
    */
  def start() {
    val ssc = StreamingContext.getActiveOrCreate(CheckpointDir, createStreamingContext)
    ssc.start()
    println("Started streaming context.")
  }
  
  /** Restart the streaming context (convenience method).
    */
  def restart() {
    stop()
    start()
  }
  
  // This method creates and initializes our stream.
  private def createStreamingContext(): StreamingContext = {
    val ssc = new StreamingContext(sc, Seconds(BatchIntervalSeconds))
    ssc.checkpoint(CheckpointDir)
    val DateFmt = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.S")

    val ds = ssc.socketTextStream(LogServerHost, LogServerPort)

    // What we're getting is a stream of log lines. As each RDD is created by
    // Spark Streaming, have the stream create a new RDD that extracts just
    // the error messages, dropping the rest.
    val keepLogsStream = ds.flatMap { line =>
      line match {
        case LogLinePattern(timeString, messageType, message) => {
          // Valid log line. Parse the date. NOTE: If this were a real
          // application, we would want to catch errors here.
          val timestamp = new Timestamp(DateFmt.parse(timeString).getTime)
          val logMessage = LogMessage(messageType, timestamp, message)
          Some(logMessage)
        }
        
        case _ => None // malformed line
      }
    }

    // Now, each RDD created by the stream will be converted to something
    // we can save to a persistent store that can easily be read, later,
    // into a DataFrame.
    keepLogsStream.foreachRDD { rdd =>
      if (! rdd.isEmpty) {
        // Using SQLContext.createDataFrame() is preferred here, since toDF()
        // can pick up (via implicits) objects that aren't serializable.
        val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
        val df = sqlContext.createDataFrame(rdd)
        df.write.mode(SaveMode.Append).parquet(OutputFile)
      }
    }
        
    // Start our streaming context.
    ssc.start()

    ssc
  }
}

// COMMAND ----------

// MAGIC %md For good measure, let's make sure our `Runner` class is serializable. If it isn't, we'll get an error when we try to start it.

// COMMAND ----------

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
val os = new ObjectOutputStream(new ByteArrayOutputStream)
os.writeObject(Runner)


// COMMAND ----------

dbutils.fs.rm(OutputDir, recurse=true)
dbutils.fs.mkdirs(OutputDir)
Runner.restart()


// COMMAND ----------

display( dbutils.fs.ls(OutputDir) )

// COMMAND ----------

// Is there anything in the output directory?
display( dbutils.fs.ls(OutputFile) )


// COMMAND ----------

// MAGIC %md
// MAGIC Let the stream run for awhile. Then, execute the following cell to stop the stream.

// COMMAND ----------

Runner.stop()

// COMMAND ----------

// MAGIC %md Okay, now let's take a look at what we have in the Parquet file. **NOTE**: This next command may fail periodically if the stream is still running.

// COMMAND ----------

val df = sqlContext.read.parquet(OutputFile)

// COMMAND ----------

// MAGIC %md How many messages did we pull down?

// COMMAND ----------

df.count

// COMMAND ----------

display(df)

// COMMAND ----------

df.registerTempTable("messages")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 1
// MAGIC 
// MAGIC How many messages of each type (ERROR, INFO, WARN) are there?
// MAGIC 
// MAGIC **HINT**: Use your DataFrame knowledge.

// COMMAND ----------

// MAGIC %sql select messageType, count(messageType) from messages group by messageType

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Exercise 2
// MAGIC 
// MAGIC Modify the `Runner`, above, to keep only the ERROR messages.

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/tmp/streaming"))

// COMMAND ----------


