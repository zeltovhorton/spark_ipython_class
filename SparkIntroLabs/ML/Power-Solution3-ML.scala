// Databricks notebook source exported at Thu, 5 Nov 2015 21:03:16 UTC
// MAGIC %md #Power Plant ML Pipeline Application
// MAGIC This is an end-to-end example of using a number of different machine learning algorithms to solve a supervised regression problem.
// MAGIC 
// MAGIC ###Table of Contents
// MAGIC 
// MAGIC - *Step 1: Business Understanding*
// MAGIC - *Step 2: Extract-Transform-Load (ETL) Your Data*
// MAGIC - *Step 3: Explore Your Data*
// MAGIC - *Step 4: Visualize Your Data*
// MAGIC - *Step 5: Data Preparation*
// MAGIC - *Step 6: Data Modeling*
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC *We are trying to predict power output given a set of readings from various sensors in a gas-fired power generation plant.  Power generation is a complex process, and understanding and predicting power output is an important element in managing a plant and its connection to the power grid.*
// MAGIC 
// MAGIC More information about Peaker or Peaking Power Plants can be found on Wikipedia https://en.wikipedia.org/wiki/Peaking_power_plant
// MAGIC 
// MAGIC 
// MAGIC Given this business problem, we need to translate it to a Machine Learning task.  The ML task is regression since the label (or target) we are trying to predict is numeric.
// MAGIC 
// MAGIC 
// MAGIC The example data is provided by UCI at [UCI Machine Learning Repository Combined Cycle Power Plant Data Set](https://archive.ics.uci.edu/ml/datasets/Combined+Cycle+Power+Plant)
// MAGIC 
// MAGIC You can read the background on the UCI page, but in summary we have collected a number of readings from sensors at a Gas Fired Power Plant
// MAGIC 
// MAGIC (also called a Peaker Plant) and now we want to use those sensor readings to predict how much power the plant will generate.
// MAGIC 
// MAGIC 
// MAGIC More information about Machine Learning with Spark can be found in the programming guide in the [SparkML Guide](https://spark.apache.org/docs/latest/mllib-guide.html)
// MAGIC 
// MAGIC 
// MAGIC *Please note this example only works with Spark version 1.4 or higher*

// COMMAND ----------

require(sc.version.replace(".", "").toInt >= 140, "Spark 1.4.0+ is required to run this notebook. Please attach it to a Spark 1.4.0+ cluster.")

// COMMAND ----------

// MAGIC %md ##Step 1: Business Understanding
// MAGIC The first step in any machine learning task is to understand the business need. 
// MAGIC 
// MAGIC As described in the overview we are trying to predict power output given a set of readings from various sensors in a gas-fired power generation plant.
// MAGIC 
// MAGIC The problem is a regression problem since the label (or target) we are trying to predict is numeric

// COMMAND ----------

// MAGIC %md ##Step 2: Extract-Transform-Load (ETL) Your Data
// MAGIC 
// MAGIC Now that we understand what we are trying to do, the first step is to load our data into a format we can query and use.  This is known as ETL or "Extract-Transform-Load".  We will load our file from Amazon s3.
// MAGIC 
// MAGIC Note: Alternatively we could upload our data using "Databricks Menu > Tables > Create Table", assuming we had the raw files on our local computer.
// MAGIC 
// MAGIC %md Our data is available on Amazon s3 at the following path:  
// MAGIC `dbfs:/databricks-datasets/power-plant/data`
// MAGIC 
// MAGIC **ToDo:** Let's start by printing the first 5 lines of the file.  
// MAGIC *Hint*: To read the file into an RDD use `sc.textFile("dbfs:/databricks-datasets/power-plant/data")`  
// MAGIC *Hint*: Then you will need to figure out how to `take` and print the first 5 lines of the RDD.

// COMMAND ----------

val rawTextRdd = sc.textFile("dbfs:/databricks-datasets/power-plant/data")
rawTextRdd.take(5).foreach(println)

// COMMAND ----------

// MAGIC %md The file is a .tsv (Tab Seperated Values) file of floating point numbers.  
// MAGIC 
// MAGIC Our schema definition from UCI appears below:
// MAGIC 
// MAGIC - AT = Atmospheric Temperature in C
// MAGIC - V = Exhaust Vacuum Speed
// MAGIC - AP = Atmospheric Pressure
// MAGIC - RH = Relative Humidity
// MAGIC - PE = Power Output.  This is the value we are trying to predict given the measurements above.
// MAGIC 
// MAGIC 
// MAGIC **ToDo:** Transform the RDD so that each row is a tuple of float values.  Then print the first 5 rows.  
// MAGIC *Hint:* Use filter to exclude lines that start with AT to remove the header.  
// MAGIC *Hint:* Use map to transform each line into a PowerPlantRow of data fields.  
// MAGIC *Hint:* Use python's str.split break up each line into individual fields.

// COMMAND ----------

case class PowerPlantTable(AT: Double, V : Double, AP : Double, RH : Double, PE : Double)
val rawDataRdd=rawTextRdd
  .map(x => x.split("\t"))
  .filter(line => line(0) != "AT")
  .map(line => PowerPlantTable(line(0).toDouble, line(1).toDouble, line(2).toDouble, line(3).toDouble, line(4).toDouble))
 val y=rawDataRdd.take(5)

// COMMAND ----------

// MAGIC %md ##Step 3: Explore Your Data
// MAGIC Now that we understand what we are trying to do, we need to load our data and describe it, explore it and verify it.

// COMMAND ----------

// MAGIC %md
// MAGIC **ToDo:** Transform your `rawDataRdd` into a Dataframe named `power_plant`.  Then use the `display(power_plant)` function to visualize it.

// COMMAND ----------

powerPlant=rawDataRdd.toDF()
display(powerPlant)

// COMMAND ----------

// MAGIC %md Next, let's register our dataframe as an SQL table.  Because this lab may be run many times, we'll take the precaution of removing any existing tables first.
// MAGIC 
// MAGIC **ToDo:** Execute the prepared code in the following cell...

// COMMAND ----------

sqlContext.sql("DROP TABLE IF EXISTS power_plant")
dbutils.fs.rm("dbfs:/user/hive/warehouse/power_plant", True)

// COMMAND ----------

// MAGIC %md **ToDo:** Register your `powerPlant` dataframe as the table named `power_plant`

// COMMAND ----------

powerPlant.registerAsTempTable("power_plant")

// COMMAND ----------

// MAGIC %sql 
// MAGIC --We can use %sql to query the rows
// MAGIC 
// MAGIC SELECT * FROM power_plant

// COMMAND ----------

// MAGIC %md We can use the SQL desc command to describe the schema

// COMMAND ----------

// MAGIC %sql desc power_plant

// COMMAND ----------

// MAGIC %md **Schema Definition**
// MAGIC 
// MAGIC Our schema definition from UCI appears below:
// MAGIC 
// MAGIC - AT = Atmospheric Temperature in C
// MAGIC - V = Exhaust Vaccum Speed
// MAGIC - AP = Atmospheric Pressure
// MAGIC - RH = Relative Humidity
// MAGIC - PE = Power Output
// MAGIC 
// MAGIC PE is our label or target. This is the value we are trying to predict given the measurements.
// MAGIC 
// MAGIC *Reference [UCI Machine Learning Repository Combined Cycle Power Plant Data Set](https://archive.ics.uci.edu/ml/datasets/Combined+Cycle+Power+Plant)*

// COMMAND ----------

// MAGIC %md **ToDo:** Display summary statistics for the the columns.  
// MAGIC *Hint:* To access the table from python use `sqlContext.table("power_plant")`  
// MAGIC *Hint:* We can use the describe function with no parameters to get some basic stats for each column like count, mean, max, min and standard deviation. The describe function is a method attached to a dataframe. More information can be found in the [Spark API docs](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrame)  

// COMMAND ----------

display(sqlContext.table("power_plant").describe())

// COMMAND ----------

// MAGIC %md ##Step 4: Visualize Your Data
// MAGIC 
// MAGIC To understand our data, we will look for correlations between features and the label.  This can be important when choosing a model.  E.g., if features and a label are linearly correlated, a linear model like Linear Regression can do well; if the relationship is very non-linear, more complex models such as Decision Trees can be better. We use Databrick's built in visualization to view each of our predictors in relation to the label column as a scatter plot to see the correlation between the predictors and the label.

// COMMAND ----------

// MAGIC %md **ToDo:** Do a scatter plot of Power(PE) as a function of Temperature (AT).  
// MAGIC *Bonus:* Name the y-axis "Power" and the x-axis "Temperature"

// COMMAND ----------

// MAGIC %sql select AT as Temperature, PE as Power from power_plant

// COMMAND ----------

// MAGIC %md Notice there appears to be a strong linear correlation between temperature and Power Output

// COMMAND ----------

// MAGIC %md **ToDo:** Do a scatter plot of Power(PE) as a function of ExhaustVacuum (V).  
// MAGIC *Bonus:* Name the y-axis "Power" and the x-axis "ExhaustVacuum"

// COMMAND ----------

// MAGIC %sql select V as ExhaustVacuum, PE as Power from power_plant;

// COMMAND ----------

// MAGIC %md The linear correlation is not as strong between Exhaust Vacuum Speed and Power Output but there is some semblance of a pattern.

// COMMAND ----------

// MAGIC %md **ToDo:** Do a scatter plot of Power(PE) as a function of Pressure (AP).  
// MAGIC *Bonus:* Name the y-axis "Power" and the x-axis "Pressure"

// COMMAND ----------

// MAGIC %sql select AP Pressure, PE as Power from power_plant;

// COMMAND ----------

// MAGIC %md **ToDo:** Do a scatter plot of Power(PE) as a function of Humidity (RH).  
// MAGIC *Bonus:* Name the y-axis "Power" and the x-axis "Humidity"

// COMMAND ----------

// MAGIC %sql select RH Humidity, PE Power from power_plant;

// COMMAND ----------

// MAGIC %md ...and atmospheric pressure and relative humidity seem to have little to no linear correlation

// COMMAND ----------

// MAGIC %md ##Step 5: Data Preparation
// MAGIC 
// MAGIC The next step is to prepare the data. Since all of this data is numeric and consistent this is a simple task for us today.
// MAGIC 
// MAGIC We will need to convert the predictor features from columns to Feature Vectors using the org.apache.spark.ml.feature.VectorAssembler
// MAGIC 
// MAGIC The VectorAssembler will be the first step in building our ML pipeline.

// COMMAND ----------

// MAGIC %md
// MAGIC **ToDo:** Assign the data frame to the local variable `dataset`  
// MAGIC **ToDo:** Create a VectorAssembler named `vectorizer` to read your dataset.

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler

val dataset = sqlContext.table("power_plant")

val vectorizer = new VectorAssembler()
vectorizer.setInputCols(Array("AT", "V", "AP", "RH"))
vectorizer.setOutputCol("features")


// COMMAND ----------

// MAGIC %md ##Step 6: Data Modeling
// MAGIC Now let's model our data to predict what the power output will be given a set of sensor readings
// MAGIC 
// MAGIC Our first model will be based on simple linear regression since we saw some linear patterns in our data based on the scatter plots during the exploration stage.
// MAGIC 
// MAGIC In machine learning we often will split up our initial data set into a "trainingSet" used to train our model and a "testSet" to evaluate the model's performance in giving predictions.
// MAGIC 
// MAGIC **ToDo:** Divide up the model into a trainingSet and a testSet.  Then cache each set into memory to maximize performance.   
// MAGIC **Hint:** Use the [randomSplit](https://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD.randomSplit) function.
// MAGIC **Hint:** The data set won't actually be cached until you perform an action on the dataset.  
// MAGIC *Tip*: For reproducability it's often best to use a predefined seed when doing the random sampling.

// COMMAND ----------

var Array(split20, split80) = dataset.randomSplit(Array(0.20, 0.80), 1800009193L)

// COMMAND ----------

// MAGIC %md Let's cache these datasets for performance

// COMMAND ----------

val testSet = split20.cache()
val trainingSet = split80.cache()

// COMMAND ----------

// MAGIC %md Next we'll create a Linear Regression Model and use the built in help to identify how to train it. See more details at [Linear Regression](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.LinearRegression) in the ML guide. 
// MAGIC 
// MAGIC **ToDo**: Run the next cell.

// COMMAND ----------

// ***** LINEAR REGRESSION MODEL ****

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.ml.Pipeline

// Let's initialize our linear regression learner
val lr = new LinearRegression()

// We use explain params to dump the parameters we can use
lr.explainParams()

// COMMAND ----------

// MAGIC %md The cell below is based on the Spark ML pipeline API. More information can be found in the Spark ML Programming Guide at https://spark.apache.org/docs/latest/ml-guide.html
// MAGIC 
// MAGIC **ToDo:** Read, understand, and then run the next cell to train our linear regression model.

// COMMAND ----------

// Now we set the parameters for the method
lr.setPredictionCol("Predicted_PE")
  .setLabelCol("PE")
  .setMaxIter(100)
  .setRegParam(0.1)


// We will use the new spark.ml pipeline API. If you have worked with scikit-learn this will be very familiar.
val lrPipeline = new Pipeline()

lrPipeline.setStages(Array(vectorizer, lr))


// Let's first train on the entire dataset to see what we get
val lrModel = lrPipeline.fit(trainingSet)



// COMMAND ----------

// MAGIC %md 
// MAGIC Since Linear Regression is Simply a Line of best fit over the data that minimizes the square of the error, given multiple input dimensions we can express each predictor as a line function of the form:
// MAGIC 
// MAGIC \\[ y = a + b x_1 + b x_2 + b x_i ... \\]
// MAGIC 
// MAGIC where a is the intercept and b are coefficients.
// MAGIC 
// MAGIC To express the coefficients of that line we can retrieve the Estimator stage from the PipelineModel and express the weights and the intercept for the function.

// COMMAND ----------

// The intercept is as follows:
val intercept = lrModel.stages(1).asInstanceOf[LinearRegressionModel].intercept

// COMMAND ----------

// The coefficents (i.e. weights) are as follows:

val weights = lrModel.stages(1).asInstanceOf[LinearRegressionModel].weights.toArray


val featuresNoLabel = dataset.columns.filter(col => col != "PE")

val coefficents = sc.parallelize(weights).zip(sc.parallelize(featuresNoLabel))

// Now let's sort the coeffecients from the most to the least

var equation = s"y = $intercept "
var variables = Array
coefficents.sortByKey().collect().foreach(x =>
  { 
        val weight = Math.abs(x._1)
        val name = x._2
        val symbol = if (x._1 > 0) "+" else "-"
          
        equation += (s" $symbol (${weight} * ${name})")
  } 
)

// Finally here is our equation
println("Linear Regression Equation: " + equation)


// COMMAND ----------

// MAGIC %md Based on examining the output it shows there is a strong negative correlation between Atmospheric Temperature (AT) and Power Output.
// MAGIC 
// MAGIC But our other dimenensions seem to have little to no correlation with Power Output. Do you remember **Step 2: Explore Your Data**? When we visualized each predictor against Power Output using a Scatter Plot, only the temperature variable seemed to have a linear correlation with Power Output so our final equation seems logical.
// MAGIC 
// MAGIC 
// MAGIC Now let's see what our predictions look like given this model.

// COMMAND ----------

val predictionsAndLabels = lrModel.transform(testSet)

display(predictionsAndLabels.select("AT", "V", "AP", "RH", "PE", "Predicted_PE"))

// COMMAND ----------

// MAGIC %md Now that we have real predictions we can use an evaluation metric such as Root Mean Squared Error to validate our regression model. The lower the Root Mean Squared Error, the better our model.

// COMMAND ----------

//Now let's compute some evaluation metrics against our test dataset

import org.apache.spark.mllib.evaluation.RegressionMetrics 

val metrics = new RegressionMetrics(predictionsAndLabels.select("Predicted_PE", "PE").rdd.map(r => (r(0).asInstanceOf[Double], r(1).asInstanceOf[Double])))

val rmse = metrics.rootMeanSquaredError
val explainedVariance = metrics.explainedVariance
val r2 = metrics.r2

println (f"Root Mean Squared Error: $rmse")
println (f"Explained Variance: $explainedVariance")  
println (f"R2: $r2")

// COMMAND ----------

// MAGIC %md Generally a good model will have 68% of predictions within 1 RMSE and 95% within 2 RMSE of the actual value. Let's calculate and see if a RMSE of 4.51 meets this criteria.

// COMMAND ----------

// First we calculate the residual error and divide it by the RMSE
predictionsAndLabels.selectExpr("PE", "Predicted_PE", "PE - Predicted_PE Residual_Error", s""" (PE - Predicted_PE) / $rmse Within_RSME""").registerTempTable("Power_Plant_RMSE_Evaluation")

// COMMAND ----------

// MAGIC %sql SELECT * from Power_Plant_RMSE_Evaluation

// COMMAND ----------

// MAGIC %sql -- Now we can display the RMSE as a Histogram. Clearly this shows that the RMSE is centered around 0 with the vast majority of the error within 2 RMSEs.
// MAGIC SELECT Within_RSME  from Power_Plant_RMSE_Evaluation

// COMMAND ----------

// MAGIC %md We can see this definitively if we count the number of predictions within + or - 1.0 and + or - 2.0 and display this as a pie chart:

// COMMAND ----------

// MAGIC %sql SELECT case when Within_RSME <= 1.0 and Within_RSME >= -1.0 then 1  when  Within_RSME <= 2.0 and Within_RSME >= -2.0 then 2 else 3 end RSME_Multiple, COUNT(*) count  from Power_Plant_RMSE_Evaluation
// MAGIC group by case when Within_RSME <= 1.0 and Within_RSME >= -1.0 then 1  when  Within_RSME <= 2.0 and Within_RSME >= -2.0 then 2 else 3 end

// COMMAND ----------

// MAGIC %md So we have 68% of our training data within 1 RMSE and 97% (68% + 29%) within 2 RMSE. So the model is pretty decent. Let's see if we can tune the model to improve it further.

// COMMAND ----------

// MAGIC %md #Step 7: Tuning and Evaluation
// MAGIC 
// MAGIC Now that we have a model with all of the data let's try to make a better model by tuning over several parameters to see if we can get better results.

// COMMAND ----------

import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.ml.evaluation._
//first let's use a cross validator to split the data into training and validation subsets


//Let's set up our evaluator class to judge the model based on the best root mean squared error
val regEval = new RegressionEvaluator()
regEval.setLabelCol("PE")
  .setPredictionCol("Predicted_PE")
  .setMetricName("rmse")

//Let's create our crossvalidator with 3 fold cross validation
val crossval = new CrossValidator()
crossval.setEstimator(lrPipeline)
crossval.setNumFolds(5)
crossval.setEvaluator(regEval)


//Let's tune over our regularization parameter from 0.01 to 0.10
val regParam = ((1 to 10) toArray).map(x => (x /100.0))

val paramGrid = new ParamGridBuilder()
  .addGrid(lr.regParam, regParam)
  .build()
crossval.setEstimatorParamMaps(paramGrid)

//Now let's create our model
val cvModel = crossval.fit(trainingSet)


// COMMAND ----------

// MAGIC %md Now that we have tuned let's see what we got for tuning parameters and what our RMSE was versus our intial model

// COMMAND ----------

val predictionsAndLabels = cvModel.transform(testSet)
val metrics = new RegressionMetrics(predictionsAndLabels.select("Predicted_PE", "PE").rdd.map(r => (r(0).asInstanceOf[Double], r(1).asInstanceOf[Double])))

val rmse = metrics.rootMeanSquaredError
val explainedVariance = metrics.explainedVariance
val r2 = metrics.r2

println (f"Root Mean Squared Error: $rmse")
println (f"Explained Variance: $explainedVariance")  
println (f"R2: $r2")

// COMMAND ----------

// MAGIC %md So our initial untuned and tuned linear regression models are statistically identical.
// MAGIC 
// MAGIC Given that the only linearly correlated variable is Temperature, it makes sense try another machine learning method such a Decision Tree to handle non-linear data and see if we can improve our model
// MAGIC 
// MAGIC A Decision Tree creates a model based on splitting variables using a tree structure. We will first start with a single decision tree model.
// MAGIC 
// MAGIC Reference Decision Trees: https://en.wikipedia.org/wiki/Decision_tree_learning

// COMMAND ----------

import org.apache.spark.ml.regression.DecisionTreeRegressor


val dt = new DecisionTreeRegressor()
dt.setLabelCol("PE")
dt.setPredictionCol("Predicted_PE")
dt.setFeaturesCol("features")
dt.setMaxBins(100)

val dtPipeline = new Pipeline()
dtPipeline.setStages(Array(vectorizer, dt))
//Let's just resuse our CrossValidator

crossval.setEstimator(dtPipeline)

val paramGrid = new ParamGridBuilder()
  .addGrid(dt.maxDepth, Array(2, 3))
  .build()
crossval.setEstimatorParamMaps(paramGrid)

val dtModel = crossval.fit(trainingSet)




// COMMAND ----------

// MAGIC %md Now let's see how our DecisionTree model compares to our LinearRegression model

// COMMAND ----------

import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.PipelineModel
 

val predictionsAndLabels = dtModel.bestModel.transform(testSet)
val metrics = new RegressionMetrics(predictionsAndLabels.select("Predicted_PE", "PE").map(r => (r(0).asInstanceOf[Double], r(1).asInstanceOf[Double])))

val rmse = metrics.rootMeanSquaredError
val explainedVariance = metrics.explainedVariance
val r2 = metrics.r2

println (f"Root Mean Squared Error: $rmse")
println (f"Explained Variance: $explainedVariance")  
println (f"R2: $r2")




// COMMAND ----------

// This line will pull the Decision Tree model from the Pipeline as display it as an if-then-else string
dtModel.bestModel.asInstanceOf[PipelineModel].stages.last.asInstanceOf[DecisionTreeRegressionModel].toDebugString

// COMMAND ----------

// MAGIC %md So our DecisionTree was slightly worse than our LinearRegression model (LR: 4.51 vs DT: 5.03). Maybe we can try an Ensemble method such as Gradient-Boosted Decision Trees to see if we can strengthen our model by using an ensemble of weaker trees with weighting to reduce the error in our model.
// MAGIC 
// MAGIC *Note since this is a complex model, the cell below can take about *16 minutes* or so to run, go out and grab a coffee and come back :-).*

// COMMAND ----------

import org.apache.spark.ml.regression.GBTRegressor

val gbt = new GBTRegressor()
gbt.setLabelCol("PE")
gbt.setPredictionCol("Predicted_PE")
gbt.setFeaturesCol("features")
gbt.setSeed(100088121L)
gbt.setMaxBins(100)
gbt.setMaxIter(120)

val gbtPipeline = new Pipeline()
gbtPipeline.setStages(Array(vectorizer, gbt))
//Let's just resuse our CrossValidator

crossval.setEstimator(gbtPipeline)

val paramGrid = new ParamGridBuilder()
  .addGrid(gbt.maxDepth, Array(2, 3))
  .build()
crossval.setEstimatorParamMaps(paramGrid)

//gbt.explainParams
val gbtModel = crossval.fit(trainingSet)

// COMMAND ----------

import org.apache.spark.ml.regression.GBTRegressionModel 

val predictionsAndLabels = gbtModel.bestModel.transform(testSet)
val metrics = new RegressionMetrics(predictionsAndLabels.select("Predicted_PE", "PE").map(r => (r(0).asInstanceOf[Double], r(1).asInstanceOf[Double])))

val rmse = metrics.rootMeanSquaredError
val explainedVariance = metrics.explainedVariance
val r2 = metrics.r2


println (f"Root Mean Squared Error: $rmse")
println (f"Explained Variance: $explainedVariance")  
println (f"R2: $r2")


// COMMAND ----------

// MAGIC %md We can use the toDebugString method to dump out what our trees and weighting look like:

// COMMAND ----------

gbtModel.bestModel.asInstanceOf[PipelineModel].stages.last.asInstanceOf[GBTRegressionModel].toDebugString

// COMMAND ----------

// MAGIC %md ### Conclusion
// MAGIC 
// MAGIC Wow! So our best model is in fact our Gradient Boosted Decision tree model which uses an ensemble of 120 Trees with a depth of 3 to construct a better model than the single decision tree.

// COMMAND ----------

// MAGIC %md #Step 8: Deployment
// MAGIC 
// MAGIC Now that we have a predictive model it is time to deploy the model into an operational environment. 
// MAGIC 
// MAGIC In our example, let's say we have a series of sensors attached the power plant and a monitoring station.
// MAGIC 
// MAGIC The monitoring station will need close to real-time information about how much power that their station will generate so they can relay that to the utility. 
// MAGIC 
// MAGIC So let's create a Spark Streaming utility that we can use for this purpose.

// COMMAND ----------

// Let's set the variable finalModel to our best GBT Model
val finalModel = gbtModel.bestModel


// COMMAND ----------

// MAGIC %md Let's create our table for predictions

// COMMAND ----------

// MAGIC %sql 
// MAGIC DROP TABLE IF EXISTS power_plant_predictions ;
// MAGIC CREATE TABLE power_plant_predictions(
// MAGIC   AT Double,
// MAGIC   V Double,
// MAGIC   AP Double,
// MAGIC   RH Double,
// MAGIC   PE Double,
// MAGIC   Predicted_PE Double
// MAGIC );

// COMMAND ----------

// MAGIC %md Now let's create our streaming job to score new power plant readings in real-time

// COMMAND ----------

import java.nio.ByteBuffer
import java.net._
import java.io._
import concurrent._
import scala.io._
import sys.process._
import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.receiver.Receiver
import sqlContext._
import net.liftweb.json.DefaultFormats
import net.liftweb.json._

import scala.collection.mutable.SynchronizedQueue


val queue = new SynchronizedQueue[RDD[String]]()

val batchIntervalSeconds = 2

var newContextCreated = false      // Flag to detect whether new context was created or not

// Function to create a new StreamingContext and set it up
def creatingFunc(): StreamingContext = {
    
  // Create a StreamingContext
  val ssc = new StreamingContext(sc, Seconds(batchIntervalSeconds))
  val batchInterval = Seconds(1)
  ssc.remember(Seconds(300))
  val dstream = ssc.queueStream(queue)
  dstream.foreachRDD { 
    rdd =>
      // if the RDD has data
       if(!(rdd.isEmpty())) {
          // Use the final model to transform a JSON message into a dataframe and pass the dataframe to our model's transform method
           finalModel
             .transform(read.json(rdd).toDF())
         // Select only columns we are interested in
         .select("AT", "V", "AP", "RH", "PE", "Predicted_PE")
         // Append the results to our power_plant_predictions table
         .write.mode(SaveMode.Append).saveAsTable("power_plant_predictions")
       } 
  }
  println("Creating function called to create new StreamingContext for Power Plant Predictions")
  newContextCreated = true  
  ssc
}

val ssc = StreamingContext.getActiveOrCreate(creatingFunc)
if (newContextCreated) {
  println("New context created from currently defined creating function") 
} else {
  println("Existing context running or recovered from checkpoint, may not be running currently defined creating function")
}

ssc.start()




// COMMAND ----------

// MAGIC %md Now that we have created and defined our streaming job, let's test it with some data. First we clear the predictions table.

// COMMAND ----------

// MAGIC %sql truncate table power_plant_predictions

// COMMAND ----------

// MAGIC %md Let's use data to see how much power output our model will predict.

// COMMAND ----------

// First we try it with a record from our test set and see what we get:
queue += sc.makeRDD(Seq(s"""{"AT":10.82,"V":37.5,"AP":1009.23,"RH":96.62,"PE":473.9}"""))

// We may need to wait a few seconds for data to appear in the table
Thread.sleep(Seconds(5).milliseconds)

// COMMAND ----------

// MAGIC %sql 
// MAGIC --and we can query our predictions table
// MAGIC select * from power_plant_predictions

// COMMAND ----------

// MAGIC %md Let's repeat with a different test measurement that our model has not seen before

// COMMAND ----------

queue += sc.makeRDD(Seq(s"""{"AT":10.0,"V":40,"AP":1000,"RH":90.0,"PE":0.0}"""))
Thread.sleep(Seconds(5).milliseconds)


// COMMAND ----------

// MAGIC %sql 
// MAGIC --Note you made to run this a couple of times to see the refreshed data...
// MAGIC select * from power_plant_predictions

// COMMAND ----------

// MAGIC %md As you can see the Predictions are very close to the real data points. 

// COMMAND ----------

// MAGIC %sql select * from power_plant where at between 10 and 11 and AP between 1000 and 1010 and RH between 90 and 97 and v between 37 and 40 order by PE 

// COMMAND ----------

// MAGIC %md Now you use the predictions table to feed a real-time dashboard or feed the utility with information on how much power the peaker plant will deliver.

// COMMAND ----------

// MAGIC %md Datasource References:
// MAGIC * Pinar Tüfekci, Prediction of full load electrical power output of a base load operated combined cycle power plant using machine learning methods, International Journal of Electrical Power & Energy Systems, Volume 60, September 2014, Pages 126-140, ISSN 0142-0615, [Web Link].
// MAGIC ([Web Link])
// MAGIC * Heysem Kaya, Pinar Tüfekci , Sadik Fikret Gürgen: Local and Global Learning Methods for Predicting Power of a Combined Gas & Steam Turbine, Proceedings of the International Conference on Emerging Trends in Computer and Electronics Engineering ICETCEE 2012, pp. 13-18 (Mar. 2012, Dubai)*

// COMMAND ----------


