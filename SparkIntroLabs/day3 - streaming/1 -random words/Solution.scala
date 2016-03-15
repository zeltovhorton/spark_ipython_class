// Databricks notebook source exported at Thu, 5 Nov 2015 17:01:41 UTC
// MAGIC %md
// MAGIC # Solution to Scala Spark Streaming Lab

// COMMAND ----------

// MAGIC %md The following solution isn't the _only_ possible solution. It's just _one_ possible solution.

// COMMAND ----------

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

    // Create a stream from our RandomWordSource
    val stream = ssc.receiverStream(createRandomWordSource())
    
    // What we're getting is a stream of words. As each RDD is created by
    // Spark Streaming, have the stream create a new RDD that filters the
    // words, dropping those larger than 7 characters or with a score less 
    // than 30.
    
    // First, map each word to a (word, score) tuple.
    stream.map { word => 
      WordScore(word, word.map(ch => LetterPoints(ch)).sum)
    }.
    // Next, filter out any word that does not have a score better than 14 and length
    // less than 8.
    filter { wordScore => (wordScore.score >= 15) && (wordScore.word.length <= 7)  }.
    // Save each RDD in our Parquet table. We'll likely end up with multiple rows for each word, but
    // we can sum those up easily enough.
    foreachRDD { rdd => 
      val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
      sqlContext.createDataFrame(rdd).write.mode(SaveMode.Append).parquet(SolutionParquet)
    }
    
    ssc
  }
}


// COMMAND ----------


