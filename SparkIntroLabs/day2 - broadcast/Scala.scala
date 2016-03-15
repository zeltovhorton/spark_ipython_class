// Databricks notebook source exported at Wed, 4 Nov 2015 19:46:07 UTC
// MAGIC %md
// MAGIC # Broadcast Variables

// COMMAND ----------

// MAGIC %md
// MAGIC ## What are Broadcast Variables?
// MAGIC Broadcast Variables allow us to broadcast a read-only copy of non-rdd data to all the executors.  The executors can then access the value of this data locally.  This is much more efficent than relying on the driver to trasmit this data teach time a task is run.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Using a Broadcast Variable

// COMMAND ----------

// Create a broadcast variable, transmitting it's value to all the executors.
var broadcastVar = sc.broadcast(1 to 3)

// I can read it's value
println(broadcastVar.value)

// When I no longer want the variable to remain on the executors, I should free up the memory.
broadcastVar.unpersist()

// COMMAND ----------

// The value is available on the driver
println(s"Driver: ${broadcastVar.value}")

// And on the executors
val results = sc.parallelize(1 to 10, numSlices=10).map { n => s"Task $n: ${broadcastVar.value}" }.collect()
println(results.mkString("\n"))


// COMMAND ----------

// MAGIC %md
// MAGIC ## How Broadcast Variables can improve performance (demo)
// MAGIC Here we have a medium sized data set, small enough to fit in RAM, but still involves quite a bit of network communication when sending the data set to the executors.

// COMMAND ----------

// Create a medium sized dataSet of several million values.
val size = 20*1000*1000
val dataSet = (1 to size).toArray

// COMMAND ----------

// MAGIC %md Now let's demonstrate the overhead of network communication when not using broadcast variables.

// COMMAND ----------

// Ceate an RDD with 5 partitions so that we can do an operation in 25 separate tasks running in parallel on up to 5 different executors.

val rdd = sc.parallelize(1 to 5, numSlices=5)
println(s"${rdd.partitions.length} partitions")

// COMMAND ----------

// In a loop, do a job 25 times without using broadcast variables...

for (i <- 1 to 25) rdd.map { x => dataSet.length * x }.collect()

// Look how slow it is...
// This is because our local "data" variable is being used by the lambda and thus must be sent to each executor every time a task is run.

// COMMAND ----------

// MAGIC %md Let's do that again, but this time we'll first send a copy of the dataset to the executors once, so that the data is available locally every time a task is run.

// COMMAND ----------

// Create a broadcast variable.  This will transmit the dataset to the executors.

val broadcastVar = sc.broadcast(dataSet)

// COMMAND ----------

// MAGIC %md Now we'll run the job 25 times, and notice how much faster it is since we don't have to retransmit the data set each time.

// COMMAND ----------

for (i <- 1 to 25) rdd.map { x => broadcastVar.value.length * x }.collect()

// COMMAND ----------

// MAGIC %md Finally, let's delete the the broadcast variable out of the Executor JVMs

// COMMAND ----------

// Free up the memory on the executors.
broadcastVar.unpersist()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Frequently Asked Questions about Broadcast Variables
// MAGIC **Q:** How is this different than using an RDD to keep data on an executor?  
// MAGIC **A:** With an RDD, the data is divided up into partitions and executors hold only a few partitions.  A broadcast variable is sent out to all the executors.
// MAGIC 
// MAGIC **Q:** When should I use an RDD and when should I use a broadcast variable?  
// MAGIC **A:** BroadCast variables must fit into RAM (and they're generally under 20 MB).  And they are on all executors.  They're good for small datasets that you can afford to leave in memory on the executors.  RDDs are better for very large datasets that you want to partition and divide up between executors.
