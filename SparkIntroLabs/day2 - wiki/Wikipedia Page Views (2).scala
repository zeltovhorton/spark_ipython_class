// Databricks notebook source exported at Wed, 4 Nov 2015 16:24:52 UTC
// MAGIC %md
// MAGIC 
// MAGIC #![Wikipedia Logo](http://sameerf-dbc-labs.s3-website-us-west-2.amazonaws.com/data/wikipedia/images/w_logo_for_labs.png)
// MAGIC 
// MAGIC # Explore English Wikipedia pageviews by second
// MAGIC ### Time to complete: 20 minutes
// MAGIC 
// MAGIC #### Business questions:
// MAGIC 
// MAGIC * Question # 1) How many rows in the table refer to *mobile* vs *desktop* site requests?
// MAGIC * Question # 2) How many total incoming requests were to the *mobile* site vs the *desktop* site?
// MAGIC * Question # 3) What is the start and end range of time for the pageviews data? How many days total of data do we have?
// MAGIC * Question # 4) Which day of the week does Wikipedia get the most traffic?
// MAGIC * Question #  5) Can you visualize both the mobile and desktop site requests together in a line chart to compare traffic between both sites by day of the week?
// MAGIC 
// MAGIC #### Technical Accomplishments:
// MAGIC 
// MAGIC * Use Spark's Scala and Python APIs
// MAGIC * Learn what a `sqlContext` is and how to use it
// MAGIC * Load a 255 MB tab separated file into a DataFrame
// MAGIC * Cache a DataFrame into memory
// MAGIC * Run some DataFrame transformations and actions to create visualizations
// MAGIC * Learn the following DataFrame operations: `show()`, `printSchema()`, `orderBy()`, `filter()`, `groupBy()`, `cast()`, `alias()`, `distinct()`, `count()`, `sum()`, `avg()`, `min()`, `max()`
// MAGIC * Write a User Defined Function (UDF)
// MAGIC * Join two DataFrames
// MAGIC * Bonus: Use Matplotlib and Python code within a Scala notebook to create a line chart
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC Dataset: http://datahub.io/en/dataset/english-wikipedia-pageviews-by-second

// COMMAND ----------

// MAGIC %md
// MAGIC ### Introduction to running Scala in Databricks Notebooks
// MAGIC 
// MAGIC Place your cursor inside the cells below, one at a time, and hit "Shift" + "Enter" to execute the code:

// COMMAND ----------

// This is a Scala cell. You can run normal Scala code here...
val x = 1 + 7

// COMMAND ----------

// Here is another Scala cell, that adds 2 to x
val y = 2 + x

// COMMAND ----------

// This line uses string interpolation to prints what y is equal to...
println(s"y is equal to ${y}")

// COMMAND ----------

// You can import additional modules and use them
import java.util.Date
println(s"This was last run on: ${new Date}")

// COMMAND ----------

// MAGIC %md
// MAGIC ### DataFrames
// MAGIC A `sqlContext` object is your entry point for working with structured data (rows and columns) in Spark.
// MAGIC 
// MAGIC Let's use the `sqlContext` to read a table of the English Wikipedia pageviews per second.

// COMMAND ----------

// Notice that the sqlContext in Databricks is actually a HiveContext
sqlContext

// COMMAND ----------

// MAGIC %md A `HiveContext` includes additional features like the ability to write queries using the more complete HiveQL parser, access to Hive UDFs, and the ability to read data from Hive tables. In general, you should always aim to use the `HiveContext` over the more limited `sqlContext`.

// COMMAND ----------

// MAGIC %md
// MAGIC Create a DataFrame named `pageviewsDF` and understand its schema:

// COMMAND ----------

// Note that we have pre-loaded the pageviews_by_second data into Databricks.
// You just have to read the existing table.

val pageviewsDF = sqlContext.read.table("pageviews_by_second")

// COMMAND ----------

// Shows the first 20 records in ASCII print
pageviewsDF.show()

// COMMAND ----------

// The display() function also shows the DataFrame, but in a prettier HTML format (this only works in Databricks notebooks)

display(pageviewsDF)

// COMMAND ----------

// MAGIC %md `printSchema()` prints out the schema, the data types and whether a column can be null:

// COMMAND ----------

pageviewsDF.printSchema()

// COMMAND ----------

// MAGIC %md Notice above that the first 2 columns are typed as `Strings`, while the requests column holds `Integers`. 

// COMMAND ----------

// MAGIC %md Also notice, in a few cells above when we displayed the table, that the rows seem to be missing chunks of time.
// MAGIC 
// MAGIC The first row shows data from March 16, 2015 at **12:09:55am**, and the second row shows data from the same day at **12:10:39am**. There appears to be missing data between those time intervals because the original data file from Wikimedia contains the data out of order and Spark read it into a DataFrame in the same order as the file.
// MAGIC 
// MAGIC Our data set does actually contain 2 rows for every second (one row for mobile site requests and another for desktop site requests).
// MAGIC 
// MAGIC We can verify this by ordering the table by the timestamp column:

// COMMAND ----------

// The following orders the rows by first the timestamp (ascending), then the site (descending) and then shows the first 10 rows

pageviewsDF.orderBy($"timestamp", $"site".desc).show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Reading from disk vs memory
// MAGIC 
// MAGIC The 255 MB pageviews file is currently on S3, which means each time you scan through it, your Spark cluster has to read the 255 MB of data remotely over the network.

// COMMAND ----------

// Count how many total records (rows) there are
pageviewsDF.count()

// COMMAND ----------

// MAGIC %md Hmm, that took about 10 - 20 seconds. Let's cache the DataFrame into memory to speed it up.

// COMMAND ----------

sqlContext.cacheTable("pageviews_by_second")


// COMMAND ----------

// MAGIC %md
// MAGIC Caching is a lazy operation (meaning it doesn't take effect until you call an action that needs to read all of the data). So let's call the `count()` action again:

// COMMAND ----------

// During this count() action, the data is not only read from S3 and counted, but also cached
pageviewsDF.count()

// COMMAND ----------

// MAGIC %md
// MAGIC The DataFrame should now be cached, let's run another `count()` to see the speed increase:

// COMMAND ----------

pageviewsDF.count()

// COMMAND ----------

// MAGIC %md Notice that operating on the DataFrame now takes less than 1 second!

// COMMAND ----------

// MAGIC %md
// MAGIC ### Exploring pageviews
// MAGIC 
// MAGIC Time to do some data analysis!

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #1:
// MAGIC **How many rows in the table refer to mobile vs desktop?**

// COMMAND ----------

pageviewsDF.filter($"site" === "mobile").count()

// COMMAND ----------

pageviewsDF.filter($"site" === "desktop").count()

// COMMAND ----------

// MAGIC %md We can also group the data by the `site` column and then call count:

// COMMAND ----------

pageviewsDF.groupBy($"site").count().show()

// COMMAND ----------

// MAGIC %md So, 3.6 million rows refer to the mobile page views and 3.6 million rows refer to desktop page views.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #2:
// MAGIC ** How many total incoming requests were to the mobile site vs the desktop site?**

// COMMAND ----------

// MAGIC %md First, let's sum up the `requests` column to see how many total requests are in the dataset.

// COMMAND ----------

// Import the sql functions package, which includes statistical functions like sum, max, min, avg, etc.
import org.apache.spark.sql.functions._

// COMMAND ----------

pageviewsDF.select(sum($"requests")).show()

// COMMAND ----------

// MAGIC %md So, there are about 13.3 billion requests total.

// COMMAND ----------

// MAGIC %md But how many of the requests were for the mobile site?

// COMMAND ----------

// MAGIC %md ** Challenge:** Using just the commands we explored above, can you figure out how to filter the DataFrame for just mobile traffic and then sum the requests column?

// COMMAND ----------

//Type in your answer below...



// COMMAND ----------

// Answer to Challenge
pageviewsDF.filter("site = 'mobile'").select(sum($"requests")).show()

// COMMAND ----------

// MAGIC %md About 4.6 billion requests were for the mobile site (and probably came from mobile phone browsers). What about the desktop site?

// COMMAND ----------

pageviewsDF.filter("site = 'desktop'").select(sum($"requests")).show()

// COMMAND ----------

// MAGIC %md About 8.7 billion requests (twice as many) were for the desktop site.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #3:
// MAGIC ** What is the start and end range of time for the pageviews data? How many days of data do we have?**

// COMMAND ----------

// MAGIC %md To accomplish this, we should first convert the `timestamp` column from a `String` type to a `Timestamp` type.

// COMMAND ----------

// Currently in our DataFrame, `pageviewsDF`, the first column is typed as a string
pageviewsDF.printSchema()

// COMMAND ----------

// MAGIC %md Create a new DataFrame, `pageviewsDF2`, that changes the timestamp column from a `string` data type to a `timestamp` data type.

// COMMAND ----------

val pageviewsDF2 = pageviewsDF.select($"timestamp".cast("timestamp").alias("timestamp"), $"site", $"requests")

// COMMAND ----------

pageviewsDF2.printSchema()

// COMMAND ----------

display(pageviewsDF2)

// COMMAND ----------

// MAGIC %md How many different years is our data from?

// COMMAND ----------

// MAGIC %md For the next command, we'll use `year()`, one of the date time function available in Spark. You can review which functions are available for DataFrames in the [Spark API docs](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$).

// COMMAND ----------

pageviewsDF2.select(year($"timestamp")).distinct().show()

// COMMAND ----------

// MAGIC %md The data only spans 2015. But which months?

// COMMAND ----------

// MAGIC %md ** Challenge:** Can you figure out how to check which months of 2015 our data covers (using the Spark API docs linked to above)?

// COMMAND ----------

//Type in your answer below...


// COMMAND ----------

//Answer to Challenge
pageviewsDF2.select(month($"timestamp")).distinct().show()

// COMMAND ----------

// MAGIC %md The data covers March and April 2015.

// COMMAND ----------

// MAGIC %md ** Challenge:** How many weeks does our data cover?
// MAGIC 
// MAGIC *Hint, check out the Date time functions available in the  [Spark API docs](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$).*

// COMMAND ----------

//Type in your answer below...


// COMMAND ----------

//Answer to Challenge
pageviewsDF2.select(weekofyear($"timestamp")).distinct().show()

// COMMAND ----------

// MAGIC %md The data set covers 6 weeks. Similarly, we can see how many days of coverage we have:

// COMMAND ----------

pageviewsDF2.select(dayofyear($"timestamp")).distinct().count()

// COMMAND ----------

// MAGIC %md We have 41 days of data.

// COMMAND ----------

// MAGIC %md To understand our data better, let's look at the average, minimum and maximum number of requests received for mobile, then desktop page views over every 1 second interval:

// COMMAND ----------

// Look at mobile statistics
pageviewsDF2.filter("site = 'mobile'").select(avg($"requests"), min($"requests"), max($"requests")).show()

// COMMAND ----------

// Look at desktop statistics
pageviewsDF2.filter("site = 'desktop'").select(avg($"requests"), min($"requests"), max($"requests")).show()

// COMMAND ----------

// MAGIC %md There certainly appears to be more requests for the desktop site.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #4:
// MAGIC ** Which day of the week does Wikipedia get the most traffic?**

// COMMAND ----------

// MAGIC %md Think about how we can accomplish this. We need to pull out the day of the week (like Mon, Tues, etc) from each row, and then sum up all of the requests by day.

// COMMAND ----------

// MAGIC %md First, use the `date_format` function to extract out the day of the week from the timestamp and rename the column as "Day of week".
// MAGIC 
// MAGIC Then we'll sum up all of the requests for each day and show the results.

// COMMAND ----------

// Notice the use of alias() to rename the new column
// "E" is a pattern in the SimpleDataFormat class in Java that extracts out the "Day in Week""

// Create a new DataFrame named pageviewsByDayOfWeekDF and cache it
val pageviewsByDayOfWeekDF = pageviewsDF2.groupBy(date_format(($"timestamp"), "E").alias("Day of week")).sum().cache()

// Show what is in the new DataFrame
pageviewsByDayOfWeekDF.show()

// COMMAND ----------

// MAGIC %md You can learn more about patterns, like "E", that [Java SimpleDateFormat](https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html) allows in the Java Docs.

// COMMAND ----------

// MAGIC %md It would help to visualize the results:

// COMMAND ----------

// This is the same command as above, except here we're tacking on an orderBy() to sort by day of week
display(pageviewsByDayOfWeekDF.orderBy($"Day of week"))

// COMMAND ----------

// MAGIC %md Click on the Bar chart icon above to convert the table into a bar chart:
// MAGIC 
// MAGIC #![Bar Chart](http://i.imgur.com/myqoDNV.png)

// COMMAND ----------

// MAGIC %md Hmm, the ordering of the days of the week is off, because the `orderBy()` operation is ordering the days of the week alphabetically. Instead of that, let's start with Monday and end with Sunday. To accomplish this, we'll need to write a short User Defined Function (UDF) to prepend each `Day of week` with a number.

// COMMAND ----------

// MAGIC %md
// MAGIC ### User Defined Functions
// MAGIC 
// MAGIC A UDF lets you code your own logic for processing column values during a DataFrame query. 
// MAGIC 
// MAGIC First, let's create a Scala match expression for pattern matching:

// COMMAND ----------

def matchDayOfWeek(day:String): String = {
  day match {
    case "Mon" => "1-Mon"
    case "Tue" => "2-Tue"
    case "Wed" => "3-Wed"
    case "Thu" => "4-Thu"
    case "Fri" => "5-Fri"
    case "Sat" => "6-Sat"
    case "Sun" => "7-Sun"
    case _ => "UNKNOWN"
  }
}


// COMMAND ----------

// MAGIC %md Test the match expression:

// COMMAND ----------

matchDayOfWeek("Tue")

// COMMAND ----------

// MAGIC %md Great, it works! Now define a UDF named `prependNumber`:

// COMMAND ----------

val prependNumberUDF = sqlContext.udf.register("prependNumber", (s: String) => matchDayOfWeek(s))

// COMMAND ----------

// Note, here is a more idomatic Scala way of registering the same UDF
// val prependNumberUDF = sqlContext.udf.register("prependNumber", matchDayOfWeek _)

// COMMAND ----------

// MAGIC %md Test the UDF to prepend the `Day of Week` column in the DataFrame with a number:

// COMMAND ----------

pageviewsByDayOfWeekDF.select(prependNumberUDF($"Day of week")).show(7)

// COMMAND ----------

// MAGIC %md Our UDF looks like it's working. Next, let's apply the UDF and also order the x axis from Mon -> Sun:

// COMMAND ----------

display((pageviewsByDayOfWeekDF.withColumnRenamed("sum(requests)", "total requests")
  .select(prependNumberUDF($"Day of week"), $"total requests")
  .orderBy("UDF(Day of week)")))

// COMMAND ----------

// MAGIC %md Click on the bar chart icon again to convert the above table into a Bar Chart.

// COMMAND ----------

// MAGIC %md Wikipedia seems to get significantly more traffic on Mondays than other days of the week.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Question #5:
// MAGIC ** Can you visualize both the mobile and desktop site requests in a line chart to compare traffic between both sites by day of the week?**

// COMMAND ----------

// MAGIC %md First, graph the mobile site requests:

// COMMAND ----------

val mobileViewsByDayOfWeekDF = pageviewsDF2.filter("site = 'mobile'").groupBy(date_format(($"timestamp"), "E").alias("Day of week")).sum().withColumnRenamed("sum(requests)", "total requests").select(prependNumberUDF($"Day of week"), $"total requests").orderBy("UDF(Day of week)").toDF("DOW", "mobile_requests")

// Cache this DataFrame
mobileViewsByDayOfWeekDF.cache()

display(mobileViewsByDayOfWeekDF)

// COMMAND ----------

// MAGIC %md Click on the bar chart icon again to convert the above table into a Bar Chart.

// COMMAND ----------

// MAGIC %md Next, graph the desktop site requests:

// COMMAND ----------

val desktopViewsByDayOfWeekDF = pageviewsDF2.filter("site = 'desktop'").groupBy(date_format(($"timestamp"), "E").alias("Day of week")).sum().withColumnRenamed("sum(requests)", "total requests").select(prependNumberUDF($"Day of week"), $"total requests").orderBy("UDF(Day of week)").toDF("DOW", "desktop_requests")

// Cache this DataFrame
desktopViewsByDayOfWeekDF.cache()

display(desktopViewsByDayOfWeekDF)

// COMMAND ----------

// MAGIC %md Click on the bar chart icon to convert the above table into a Bar Chart.

// COMMAND ----------

// MAGIC %md Now that we have two DataFrames (one for mobile views by day of week and another for desktop views), let's join both of them to create one line chart to visualize mobile vs. desktop page views:

// COMMAND ----------

display(mobileViewsByDayOfWeekDF.join(desktopViewsByDayOfWeekDF, mobileViewsByDayOfWeekDF("DOW") === desktopViewsByDayOfWeekDF("DOW")))

// COMMAND ----------

// MAGIC %md Click on the line chart icon above to convert the table into a line chart:
// MAGIC 
// MAGIC #![Line Chart](http://i.imgur.com/eXjxL5x.png)

// COMMAND ----------

// MAGIC %md Then click on Plot Options:
// MAGIC 
// MAGIC #![Plot Options](http://i.imgur.com/sASSo9f.png)

// COMMAND ----------

// MAGIC %md Finally customize the plot as seen below and click Apply:
// MAGIC 
// MAGIC *(You will have to drag and drop fields from the left pane into either Keys or Values)*
// MAGIC 
// MAGIC #![Customize Plot](http://i.imgur.com/VIyNNoA.png)

// COMMAND ----------

// MAGIC %md Hmm, did you notice that the line chart is a bit deceptive? Beware that it looks like there were almost zero mobile site requests because the y-axis of the line graph starts from 600,000,000 instead of 0.
// MAGIC 
// MAGIC #![Customize Plot](http://i.imgur.com/YiThldl.png)

// COMMAND ----------

// MAGIC %md Since the y-axis is off, it may appear as if there were almost zero mobile site requests. We can restore a zero baseline by using Matplotlib.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Bonus:
// MAGIC ** Use Matplotlib to fix the line chart visualization above so that the y-axis starts with 0 **

// COMMAND ----------

// MAGIC %md Databricks notebooks let you move seemlessly between Scala and Python code within the same notebook by using `%python` to declare python cells:

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC # Create a function named simpleMath
// MAGIC def simpleMath(x, y):
// MAGIC   z = x + y
// MAGIC   print "z is: ", z

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC simpleMath(2, 3)

// COMMAND ----------

// MAGIC %md You can also import Matplotlib and easily create more sophisticated plots:

// COMMAND ----------

// MAGIC %md Note that learning Matplotlib is beyond the scope of this class, but you should get a idea of the power of integrating Mapplotlib in your notebooks by looking at the cells below.

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC import numpy as np
// MAGIC import matplotlib.pyplot as plt
// MAGIC 
// MAGIC fig1, ax = plt.subplots()
// MAGIC 
// MAGIC # The first list of four numbers is for the x-axis and the next list is for the y-axis
// MAGIC ax.plot([1,2,3,4], [1,4,9,16])
// MAGIC 
// MAGIC display(fig1)

// COMMAND ----------

// MAGIC %md Recall that we had earlier cached 2 DataFrames, one with desktop views by day of week and another with mobile views by day of week:

// COMMAND ----------

// MAGIC %md First let's graph only the desktop views by day of week:

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC fig2, ax = plt.subplots()
// MAGIC 
// MAGIC # Notice that we are providing the coordinate manually for the x-axis
// MAGIC ax.plot([0,1,2,3,4,5,6], [1566792176,1346947425,1346330702,1306170813,1207342832,1016427413,947169611], 'ro')
// MAGIC 
// MAGIC # The axis() command takes a list of [xmin, xmax, ymin, ymax] and specifies the viewport of the axes
// MAGIC ax.axis([0, 7, 0, 2000000000])
// MAGIC 
// MAGIC display(fig2)

// COMMAND ----------

desktopViewsByDayOfWeekDF.show()

// COMMAND ----------

mobileViewsByDayOfWeekDF.show()

// COMMAND ----------

// MAGIC %md Finally, let's combine the 2 plots above and also programatically get the requests data from a DataFrame (instead of manually entering the y-axis corrdinates).
// MAGIC 
// MAGIC We need a technique to access the Scala DataFrames from the Python cells. To do this, we can register a temporary table in Scala, then call that table from Python.

// COMMAND ----------

mobileViewsByDayOfWeekDF.registerTempTable("mobileViewsByDOW")
desktopViewsByDayOfWeekDF.registerTempTable("desktopViewsByDOW")

// COMMAND ----------

// MAGIC %md Next graph only the mobile views by day of week:

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC fig3, ax = plt.subplots()
// MAGIC ax.plot([0,1,2,3,4,5,6], [790026669,648087459,631284694,625338164,635169886,646334635,629556455], 'bo')
// MAGIC 
// MAGIC # The axis() command takes a list of [xmin, xmax, ymin, ymax] and specifies the viewport of the axes
// MAGIC ax.axis([0, 7, 0, 2000000000])
// MAGIC 
// MAGIC display(fig3)

// COMMAND ----------

// MAGIC %md We now have our two Python lists::

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC pythonListForMobileRequests

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC mobileViewsPythonDF = sqlContext.read.table("mobileViewsByDOW")
// MAGIC 
// MAGIC pythonListForMobileAll = [list(r) for r in mobileViewsPythonDF.collect()]
// MAGIC 
// MAGIC pythonListForMobileRequests = []
// MAGIC 
// MAGIC for item in pythonListForMobileAll:
// MAGIC         pythonListForMobileRequests.append(item[1])
// MAGIC 
// MAGIC pythonListForMobileRequests

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC desktopViewsPythonDF = sqlContext.read.table("desktopViewsByDOW")
// MAGIC 
// MAGIC pythonListForDesktopAll = [list(r) for r in desktopViewsPythonDF.collect()]
// MAGIC 
// MAGIC pythonListForDesktopRequests = []
// MAGIC 
// MAGIC for item in pythonListForDesktopAll:
// MAGIC         pythonListForDesktopRequests.append(item[1])
// MAGIC 
// MAGIC pythonListForDesktopRequests

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC pythonListForDesktopRequests

// COMMAND ----------

// MAGIC %md Finally, we are ready to plot both Desktop and Mobile requests using our python lists:

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC fig3, ax = plt.subplots()
// MAGIC 
// MAGIC x_axis = [0,1,2,3,4,5,6]
// MAGIC 
// MAGIC ax.plot(x_axis, pythonListForDesktopRequests, marker='o', linestyle='--', color='r', label='Desktop')
// MAGIC ax.plot(x_axis, pythonListForMobileRequests, marker='o', label='Mobile')
// MAGIC 
// MAGIC ax.set_title('Desktop vs Mobile site requests')
// MAGIC 
// MAGIC ax.set_xlabel('Days of week')
// MAGIC ax.set_ylabel('# of requests')
// MAGIC 
// MAGIC ax.legend()
// MAGIC 
// MAGIC # The axis() command takes a list of [xmin, xmax, ymin, ymax] and specifies the viewport of the axes
// MAGIC ax.axis([0, 6, 0, 2000000000])
// MAGIC 
// MAGIC ax.xaxis.set_ticks(range(len(x_axis)), ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
// MAGIC 
// MAGIC display(fig3)

// COMMAND ----------

// MAGIC %md This concludes the Pageviews lab.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Homework:
// MAGIC ** Recreate the same Matplotlib chart above, but instead of plotting the 7 days of the week on the x-axis, change it to 24 hours of the day. Try to visualize how much traffic Wikipedia gets to its mobile site vs. the desktop site by hour of day.**

// COMMAND ----------


