// Databricks notebook source exported at Thu, 5 Nov 2015 19:27:37 UTC
// MAGIC %md
// MAGIC # Solutions to Lab Exercises

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 1

// COMMAND ----------

// MAGIC %sql select messageType, count(messageType) as total from messages group by messageType

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercise 2
// MAGIC 
// MAGIC ### Solution (a)
// MAGIC 
// MAGIC Modify the `flatMap` block as follows:

// COMMAND ----------

val keepLogsStream = ds.flatMap { line =>
  line match {
    // The following line is the change.
    case LogLinePattern(timeString, messageType, message) if messageType == "ERROR" => {
      val timestamp = new Timestamp(DateFmt.parse(timeString).getTime)
      val logMessage = LogMessage(messageType, timestamp, message)
      Some(logMessage)
    }
        
    case _ => None // malformed line
  }
}

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Solution (b)
// MAGIC 
// MAGIC You could also add a `filter` operation to the original, unmodified `keepLogsStream`:

// COMMAND ----------

val filteredStream = keepLogsStream.filter { message => message.messageType == "ERROR" }

// COMMAND ----------

// MAGIC %md Then, you'd have to modify the `foreachRDD` block so that it runs against `filteredStream`, instead of `keepLogsStream`.
// MAGIC 
// MAGIC Solution (a) is likely to be more efficient, however.

// COMMAND ----------


