// Databricks notebook source exported at Wed, 4 Nov 2015 20:17:17 UTC
// MAGIC %md 
// MAGIC #Solution to Accumulators Lab Exercise #1:

// COMMAND ----------

val file = sc.textFile("/databricks-datasets/README.md")
val blankLines = sc.accumulator(0)
val allLines = file.map(line => 
{
  if (line == "") {
    blankLines += 1  // For debug only, not production
  }
  line // Just return the line
})
allLines.count // Everything above is lazy
println("Number of blanklines: " + blankLines.value)

// COMMAND ----------

// MAGIC %md #Solution to Accumulators Lab Exercise #2:

// COMMAND ----------

val file = sc.textFile("/databricks-datasets/README.md")
val blankLines = sc.accumulator(0)
val nonBlankLines = file.filter { line => // line coming in as before...
  
  if (line == ""){
  	blankLines += 1
  } 
  
  line != ""
} 

nonBlankLines.count // force evaluation of the RDDs using an action

println("Number of blanklines: " + blankLines.value)

// COMMAND ----------

// MAGIC %md #Solution to Broadcast Variables Lab Exercise: 

// COMMAND ----------

val bvs = sc.broadcast("... is Amazing")
val wordsRDD = sc.parallelize(List("Andy" , "Mehera", "Don", "Jessica", "Baba", "Vanessa", "Sameer", "Donnita", "Brian", "Arwen", "Doug", "Galadriel", "Yaakov"))
wordsRDD.filter( name =>
  name.contains("Don")
)
.collect()
.foreach( name => 
 println(name + " : " + bvs.value)
)

// COMMAND ----------


