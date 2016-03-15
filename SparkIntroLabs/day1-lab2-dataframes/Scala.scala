// Databricks notebook source exported at Wed, 4 Nov 2015 13:23:53 UTC
// MAGIC %md
// MAGIC # SQL and DataFrames: Hands-on Exercises (Scala)
// MAGIC 
// MAGIC This notebook contains hands-on exercises used in conjunction with the DataFrames module. Each section corresponds to a section in the lecture. Your instructor will tell you when it's time to do each section of this notebook.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Schema Inference
// MAGIC 
// MAGIC In this exercise, let's explore schema inference. We're going to be using a file called `people.txt`. The data is structured, but it has no self-describing schema. And, it's not JSON, so Spark can't infer the schema automatically. Let's create an RDD and look at the first few rows of the file.

// COMMAND ----------

val rdd = sc.textFile("dbfs:/mnt/databricks-corp-training/common/dataframes/people.txt")
rdd.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md As you can see, each line consists of the same information about a person:
// MAGIC 
// MAGIC * first name
// MAGIC * middle name
// MAGIC * last name
// MAGIC * gender ("M" or "F")
// MAGIC * birth date, in `yyyy-mm-dd` form
// MAGIC * a salary
// MAGIC * a United States Social Security Number
// MAGIC 
// MAGIC (Before you get _too_ excited and run out to apply for a bunch of credit cards, the Social Security Numbers are all fake.)
// MAGIC 
// MAGIC Clearly, the file has a schema, but Spark can't figure out what it is.
// MAGIC 
// MAGIC Read through the following code to see how we can apply a schema to the file. Then, run it, and see what happens.

// COMMAND ----------

import sqlContext.implicits._
import java.util.Date
import java.text.SimpleDateFormat
import java.sql.Timestamp

val dateFmt = new SimpleDateFormat("yyyy-MM-dd")

case class Person(firstName:  String,
                  middleName: String,
                  lastName:   String,
		          gender:     String,
                  birthDate:  Timestamp,
                  salary:     Int,
                  ssn:        String)

val peopleRDD = rdd.map { line =>
  val cols = line.split(":")
  Person(firstName  = cols(0),
         middleName = cols(1), 
         lastName   = cols(2), 
         gender     = cols(3),
         birthDate  = new Timestamp(dateFmt.parse(cols(4)).getTime),
         salary     = cols(5).toInt,
         ssn        = cols(6))
}

val df = peopleRDD.toDF

// COMMAND ----------

// MAGIC %md
// MAGIC **Question:** What could go wrong in the above code? How would you fix the problems?

// COMMAND ----------

// MAGIC %md Now, let's sample some of the data.

// COMMAND ----------

val sampledDF = df.sample(withReplacement = false, fraction = 0.02, seed = 1887348908234L)
display(sampledDF)

// COMMAND ----------

// MAGIC %md Finally, let's run a couple SQL commands.

// COMMAND ----------

df.registerTempTable("people")

// COMMAND ----------

// MAGIC %sql SELECT * FROM people WHERE birthDate >= '1970-01-01' AND birthDate <= '1979-12-31' ORDER BY birthDate, salary

// COMMAND ----------

// MAGIC %sql SELECT concat(firstName, " ", lastName) AS name, gender, year(birthDate) AS birthYear, salary FROM people WHERE salary < 50000

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![](http://i.imgur.com/RdABwEB.png) STOP HERE.
// MAGIC 
// MAGIC Let's switch back to the slides.

// COMMAND ----------

// MAGIC %md
// MAGIC ## select and filter (and a couple more)
// MAGIC 
// MAGIC We've now seen `printSchema()`, `show()`, `select()` and `filter()`. Let's take them for a test drive.
// MAGIC 
// MAGIC First, let's look at the schema.

// COMMAND ----------

df.printSchema()

// COMMAND ----------

// MAGIC %md Now, let's look at `show()`.

// COMMAND ----------

df.show() // show the first 20 rows of the DataFrame

// COMMAND ----------

// MAGIC %md `show()` is a good way to get a quick feel for your data. Of course, in a Databricks notebook, the `display()` helper is better. However, if you're using `spark-shell`, the `display()` helper isn't available.

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's look at `select()`. Run the following cell. What does it return?

// COMMAND ----------

df.select($"firstName", $"lastName", $"gender")

// COMMAND ----------

// MAGIC %md
// MAGIC **Remember**: Transformations are _lazy_. The `select()` method is a transformation.
// MAGIC 
// MAGIC All right. Let's look at result of a `select()` call.

// COMMAND ----------

df.select($"firstName", $"lastName", $"gender").show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC Finally, let's take a look at `filter()`, which can be used to filter data _out_ of a data set.
// MAGIC 
// MAGIC **Question**: What the does the following code actually do?

// COMMAND ----------

df.filter($"gender" === "M")

// COMMAND ----------

// MAGIC %md Note the use of a triple-equals (`===`) there. In Scala, that's required. You'll get a compiler error if you use `==`. (Try it.) If you like to switch between Python and Scala, be aware that you use double-equals (`==`) in Python and triple-equals in Scala.
// MAGIC 
// MAGIC `filter()`, like `select()`, is a transformation: It's _lazy_.
// MAGIC 
// MAGIC Let's try something a little more complicated. Let's combine two `filter()` operations with a `select()`, displaying the results.

// COMMAND ----------

val df2 = df.filter($"gender" === "M").filter($"salary" > 100000).select($"firstName", $"lastName", $"salary")
display(df2)

// COMMAND ----------

// MAGIC %md
// MAGIC ### ![](http://i.imgur.com/RdABwEB.png) STOP HERE.
// MAGIC 
// MAGIC Let's switch back to the slides.

// COMMAND ----------

// MAGIC %md 
// MAGIC ## orderBy, groupBy and alias

// COMMAND ----------

// MAGIC %md Up in the first section of this notebook, we ran this SQL statement:
// MAGIC 
// MAGIC ```
// MAGIC SELECT * FROM people WHERE birthDate >= '1970-01-01' AND birthDate <= '1979-12-31' ORDER BY birthDate, salary
// MAGIC ```
// MAGIC 
// MAGIC Let's try that same query with the programmatic DataFrames API.

// COMMAND ----------

display( df.filter($"birthDate" >= "1970-01-01" && $"birthDate" <= "1979-12-31").orderBy(df("birthDate"), df("salary")) )

// COMMAND ----------

// MAGIC %md There are several things to note.
// MAGIC 
// MAGIC 1. This time, we _combined_ two filter expressions into one `filter()` call, instead of chaining two `filter()` calls.
// MAGIC 2. We did not have to convert the date literals ("1970-01-01" and "1979-12-31") into `java.sql.Timestamp` objects before using them in the comparisons.
// MAGIC 3. We used two different ways to specify the columns: `$("firstName")` and `df("firstName")`.
// MAGIC 
// MAGIC Let's try a `groupBy()` next.

// COMMAND ----------

display( df.groupBy($"salary") )

// COMMAND ----------

// MAGIC %md Okay, that didn't work. Note that `groupBy()` returns something of type `GroupedData`, instead of a `DataFrame`. There are other methods on `GroupedData` that will convert back to a DataFrame. A useful one is `count()`.
// MAGIC 
// MAGIC **WARNING**: Don't confuse `GroupedData.count()` with `DataFrame.count()`. `GroupedData.count()` is _not_ an action. `DataFrame.count()` _is_ an action.

// COMMAND ----------

val x = df.groupBy($"salary").count()  // What is x?

// COMMAND ----------

display(x)

// COMMAND ----------

// MAGIC %md Let's add a filter and, while we're at it, rename the `count` column.

// COMMAND ----------

display( df.groupBy($"salary").count().filter($"count" > 1).select($"salary", $"count".as("total")) )

// COMMAND ----------


