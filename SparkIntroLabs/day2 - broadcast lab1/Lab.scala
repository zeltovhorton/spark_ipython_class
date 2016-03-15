// Databricks notebook source exported at Wed, 4 Nov 2015 20:17:08 UTC
// MAGIC %md
// MAGIC # Accumulators and Broadcast Variables (Scala)
// MAGIC 
// MAGIC ## Lab Exercise #1:
// MAGIC 
// MAGIC First and foremost we need to work with an RDD. Let's wrap an RDD around the file: README.md from the Databricks distribution.
// MAGIC 
// MAGIC The easiest way to do this is with the `SparkContext` (hereafter just: `sc`) `textFile(...)` method, which takes the path of a file on the Databricks server to which you are now logged in. The path that works is: `dbfs:/databricks-datasets/README.md`
// MAGIC 
// MAGIC The goal of this lab exercise is to count the number of "blank lines" found in the text file.

// COMMAND ----------

val file = // what goes here?

// COMMAND ----------

// MAGIC %md
// MAGIC ## Great work! 
// MAGIC 
// MAGIC Now, we need an Accumulator instance to work with which we will be sending over to the Function. We get an Accumulator from the SparkContext itself, by calling `accumulator(...)` method and passing in the initial value for that Accumulator. Try to do that now. In our case, we want to count the number of "blank lines" in the text file:

// COMMAND ----------

val blankLines = // what goes here?

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Yes, good!
// MAGIC 
// MAGIC Now comes the tricky part. You have an `Accumulator` instance ready to work with, and all that is needed is to map the lines in the file you have (the text file), through some validation to see if the line that is coming in is blank or not. Remember that file is an RDD of lines of the text file `README.md`. Using the `map(...)` transformation, check to see if the line is blank, and if it is, add one to the `Accumulator` instance. 
// MAGIC 
// MAGIC **NOTE:** In real production code, we would do this work in the action (not the transformation) but that's okay for now.

// COMMAND ----------

val allLines = file.map( // what is coming in to our function?
{
  if (/* something */ == "") {
   
    //add to the accumulator you got before
  }
  line // Just return the line
})

allLines.count // Everything above is lazy so call an action
println("Number of blanklines: " + blankLines.value) // show that it worked

  

// COMMAND ----------

// MAGIC %md 
// MAGIC ## You did it!
// MAGIC 
// MAGIC ## This is Accumulators in Scala Lab Exercise #2
// MAGIC 
// MAGIC The work you just did works, but we now want to do the same type of work more effiently (to be more robust in production). Let's try to refactor that code to run as a filter() instead:

// COMMAND ----------

val file = sc.textFile("dbfs:/databricks-datasets/README.md")
val blankLines = sc.accumulator(0)
val nonBlankLines = file.filter { line => // line coming in as before...
  
  	// If the line is blank add to the accumulator
  } 
  // If the line is blank return false and only nonBlankLines will be added to the resulting RDD
}
nonBlankLines.count // force evaluation of the RDDs using an action

println("Number of blanklines: " + blankLines.value)


// COMMAND ----------

// MAGIC %md 
// MAGIC ## This is the Broadcast Variables Lab Exercise #1
// MAGIC 
// MAGIC What we want to create is a variable that we can concatenate to the end of the name. Like a String with the value, "... is Amazing!" because we love someone.
// MAGIC Let's also have a list of names, and filter the names only selecting some by a pattern. You can do this however you want, but we suggest two people Don and Donnita, because both start with Don.
// MAGIC When we work with this RDD, remember it lives on the cluster, so if our function uses a println, the name and ... is Amazing! will not appear in the Databricks console, because the console is really the driver.
// MAGIC 
// MAGIC So, in order for this to work, invoke `collect()` before you use the `foreach(...)` to `println(...)` the name plus the value of the broadcast variable.

// COMMAND ----------

val bvs = sc.broadcast(/* a string to add to the end of the name */)
val wordsRDD = sc.parallelize(List("Andy" , "Mehera", "Don", /* add more names to this list */))

/*
 narrow your list of names by using a filter(...) transformation on your RDD 
 to see if the name contains the pattern you are looking for ...
*/
.collect() // bring the collection to the driver
.foreach( name => 
 println(name + " : " + /* the value of your broadcast variable */)
)




  
