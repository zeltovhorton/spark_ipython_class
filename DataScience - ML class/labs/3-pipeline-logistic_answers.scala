// Databricks notebook source exported at Tue, 16 Feb 2016 19:44:54 UTC
// MAGIC %md
// MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

// COMMAND ----------

// MAGIC %md
// MAGIC # Pipelines and Logistic Regression
// MAGIC  
// MAGIC In this lab we'll cover transformers, estimators, evaluators, and pipelines.  We'll use transformers and estimators to prepare our data for use in a logistic regression model and will use pipelines to combine these steps together.  Finally, we'll evaluate our model.
// MAGIC  
// MAGIC This lab also covers creating train and test datasets using `randomSplit`, visualizing a ROC curve, and generating both `ml` and `mllib` logistic regression models.
// MAGIC  
// MAGIC After completing this lab you should be comfortable using transformers, estimators, evaluators, and pipelines.

// COMMAND ----------

val baseDir = "/mnt/ml-class/"
val irisTwoFeatures = sqlContext.read.parquet(baseDir + "irisTwoFeatures.parquet").cache
irisTwoFeatures.take(2).foreach(println)

// COMMAND ----------

display(irisTwoFeatures)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Prepare the data
// MAGIC  
// MAGIC To explore our data in more detail, we're going to we pull out sepal length and sepal width and create two columns.  These are the two features found in our `DenseVector`.
// MAGIC  
// MAGIC In order to do this you will write a `udf` that takes in two values.  The first will be the name of the vector that we are operating on and the second is a literal for the index position.  Here are links to `lit` in the [Python](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.lit) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) APIs.
// MAGIC  
// MAGIC The `udf` will return a `DoubleType` that is the value of the specified vector at the specified index position.
// MAGIC  
// MAGIC In order to call our function, we need to wrap the second value in `lit()` (e.g. `lit(1)` for the second element).  This is because our `udf` expects a `Column` and `lit` generates a `Column` where the literal is the value.

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.functions.{udf, lit}
import org.apache.spark.mllib.linalg.Vector
 
val getElement = udf { (v: Vector, i: Int) => v(i) }
 
val irisSeparateFeatures = (irisTwoFeatures
                            .withColumn("sepalLength", getElement($"features", lit(0)))
                            .withColumn("sepalWidth", getElement($"features", lit(1))))
display(irisSeparateFeatures)

// COMMAND ----------

import org.apache.spark.mllib.linalg.Vector
// TEST
val firstRow = irisSeparateFeatures.select("sepalWidth", "features").map(r => (r.getAs[Double](0), r.getAs[Vector](1))).first
assert(firstRow._1 == firstRow._2(1), "incorrect definition for getElement")

// COMMAND ----------

// MAGIC %md
// MAGIC What about using `Column`'s `getItem` method?

// COMMAND ----------

try {
  display(irisTwoFeatures.withColumn("sepalLength", $"features".getItem(0)))
} catch {
  case ae: org.apache.spark.sql.AnalysisException => println(ae)
}

// COMMAND ----------

// MAGIC %md
// MAGIC Unfortunately, it doesn't work for vectors, but it does work on arrays.

// COMMAND ----------

val arrayDF = sqlContext.createDataFrame(Seq((Array(1,2,3), 0), (Array(3,4,5), 1))).toDF("anArray", "index")
arrayDF.show
 
arrayDF.select($"anArray".getItem(0)).show()
arrayDF.select($"anArray"(1)).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Part 2

// COMMAND ----------

// MAGIC %md
// MAGIC Next, let's register our function and then call it directly from SQL.

// COMMAND ----------

import org.apache.spark.mllib.linalg.Vector
 
sqlContext.udf.register("getElement", getElement.f.asInstanceOf[Function2[Vector, Int, Double]])
irisTwoFeatures.registerTempTable("irisTwo")

// COMMAND ----------

// MAGIC %sql
// MAGIC select getElement(features, 0) as sepalLength from irisTwo

// COMMAND ----------

// MAGIC %md
// MAGIC #### EDA and feature engineering

// COMMAND ----------

// MAGIC %md
// MAGIC Let's see the ranges of our values and view their means and standard deviations.

// COMMAND ----------

// MAGIC %md
// MAGIC Our features both take on values from -1.0 to 1.0, but have different means and standard deviations.  How could we standardize our data to have zero mean and unit standard deviations?  For this task we'll use the `ml` estimator `StandardScaler`.  Feature transformers (which are sometimes estimators) can be found in [pyspark.ml.feature](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.feature) for Python or [org.apache.spark.ml.feature](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.feature.package) for Scala.
// MAGIC  
// MAGIC Also, remember that the [ML Guide](http://spark.apache.org/docs/latest/ml-features.html#standardscaler) is a good place to find additional information.

// COMMAND ----------

import org.apache.spark.ml.feature.StandardScaler
 
val standardScaler = new StandardScaler()
  .setInputCol("features")
  .setOutputCol("standardized")
  .setWithMean(true)
 
println(standardScaler.explainParams() + "\n\n")

// COMMAND ----------

val irisStandardizedLength = standardScaler
  .fit(irisSeparateFeatures)
  .transform(irisSeparateFeatures)
  .withColumn("standardizedLength", getElement($"standardized", lit(0)))
display(irisStandardizedLength)

// COMMAND ----------

display(irisStandardizedLength.describe("sepalLength", "standardizedLength"))

// COMMAND ----------

// MAGIC %md
// MAGIC What if instead we wanted to normalize the data?  For example, we might want to normalize each set of features (per row) to have length one using an \\( l^2 \\) norm.  That would cause the sum of the features squared to be one: \\( \sum_{i=1}^d x_i^2 = 1 \\).  This is could be useful if we wanted to compare observations based on a distance metric like in k-means clustering.
// MAGIC  
// MAGIC `Normalizer` can be found in [pyspark.ml.feature](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.Normalizer) for Python and the [org.apache.spark.ml.feature](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.Normalizer) package for Scala.
// MAGIC  
// MAGIC Let's implement `Normalizer` and transform our features.  Make sure to use a `P` of 2.0 and to name the output column "featureNorm".  Remember that we're working with the `irisTwoFeatures` dataset.

// COMMAND ----------

// ANSWER
import org.apache.spark.ml.feature.Normalizer
 
val normalizer = new Normalizer()
  .setInputCol("features")
  .setOutputCol("featureNorm")
  .setP(2.0)
 
val irisNormalized = normalizer.transform(irisTwoFeatures)  // Note that we're calling transform here
display(irisNormalized)

// COMMAND ----------

// TEST
import org.apache.spark.mllib.linalg.Vectors
val firstVector = irisNormalized.select("featureNorm").map(_.getAs[Vector](0)).first
val vectorNorm = Vectors.norm(firstVector, 2.0)
 
assert(math.round(vectorNorm) == 1.0,
       "incorrect setup of normalizer")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Part 3

// COMMAND ----------

// MAGIC %md
// MAGIC Let's just check and see that our norms are equal to 1.0

// COMMAND ----------

import org.apache.spark.mllib.linalg.{Vector, Vectors}
 
val l2Norm = udf { v: Vector => Vectors.norm(v, 2.0) }
 
val featureLengths = irisNormalized.select(l2Norm($"features").alias("featuresLength"),
                                           l2Norm($"featureNorm").alias("featureNormLength"))
display(featureLengths)

// COMMAND ----------

// MAGIC %md
// MAGIC Next, let's bucketize our features.  This will allow us to convert continuous features into discrete buckets.  This is often desirable for logistic regression which we'll be performing later in this lab.
// MAGIC  
// MAGIC We'll use the following splits: -infinity, -0.5, 0.0, 0.5, +infinity.  Note that in Python infinity can be represented using `float('inf')` and that in Scala `Double.NegativeInfinity` and `Double.PositiveInfinity` can be used.

// COMMAND ----------

import org.apache.spark.ml.feature.Bucketizer
 
val splits = Array(Double.NegativeInfinity, -.5, 0.0, .5, Double.PositiveInfinity)
 
val lengthBucketizer = new Bucketizer()
  .setInputCol("sepalLength")
  .setOutputCol("lengthFeatures")
  .setSplits(splits)
 
val irisBucketizedLength = lengthBucketizer.transform(irisSeparateFeatures)
display(irisBucketizedLength)

// COMMAND ----------

val widthBucketizer = new Bucketizer()
  .setInputCol("sepalWidth")
  .setOutputCol("widthFeatures")
  .setSplits(splits)
 
val irisBucketizedWidth = widthBucketizer.transform(irisBucketizedLength)
display(irisBucketizedWidth)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's combine the two bucketizers into a [Pipeline](http://spark.apache.org/docs/latest/ml-guide.html#pipeline-components) that performs both bucketizations.  A `Pipeline` is made up of stages which can be set using `setStages` and passing in a `list` of stages in Python or an `Array` of stages in Scala.  `Pipeline` is an estimator, which means it implements a `fit` method which returns a `PipelineModel`.  A `PipelineModel` is a transformer, which means that it implements a `transform` method which can be used to run the stages.

// COMMAND ----------

import org.apache.spark.ml.Pipeline
 
val pipelineBucketizer = new Pipeline().setStages(Array(lengthBucketizer, widthBucketizer))
 
val pipelineModelBucketizer = pipelineBucketizer.fit(irisSeparateFeatures)
val irisBucketized = pipelineModelBucketizer.transform(irisSeparateFeatures)
 
display(irisBucketized)

// COMMAND ----------

// MAGIC %md
// MAGIC Now that we have created two new features through bucketing, let's combine those two features into a `Vector` with `VectorAssembler`.  VectorAssembler can be found in [pyspark.ml.feature](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.VectorAssembler) for Python and the [org.apache.spark.ml.feature](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.VectorAssembler) package for Scala.
// MAGIC  
// MAGIC Set the params of `assembler` so that both "lengthFeatures" and "widthFeatures" are assembled into a column called "featuresBucketized".
// MAGIC  
// MAGIC Then, set the stages of `pipeline` to include both bucketizers and the assembler as the last stage.
// MAGIC  
// MAGIC Finally, use `pipeline` to generate a new `DataFrame` called `irisAssembled`.

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler
val pipeline = new Pipeline()
val assembler = new VectorAssembler()
 
println(assembler.explainParams())
println("\n" + pipeline.explainParams() + "\n\n")

// COMMAND ----------

// ANSWER
// Set assembler params
assembler
  .setInputCols(Array("lengthFeatures", "widthFeatures"))
  .setOutputCol("featuresBucketized")
 
pipeline.setStages(Array(lengthBucketizer, widthBucketizer, assembler))
val irisAssembled = pipeline.fit(irisSeparateFeatures).transform(irisSeparateFeatures)
display(irisAssembled)

// COMMAND ----------

// TEST
import org.apache.spark.mllib.linalg.Vectors
val firstAssembly = irisAssembled.select("lengthFeatures", "widthFeatures", "featuresBucketized").first
assert(Vectors.dense(Array(firstAssembly.getAs[Double](0), firstAssembly.getAs[Double](1))) == firstAssembly(2),
       "incorrect value for column featuresBucketized")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Part 4

// COMMAND ----------

// MAGIC %md
// MAGIC  
// MAGIC #### Logistic Regression

// COMMAND ----------

// MAGIC %md
// MAGIC First let's look at our data by label.

// COMMAND ----------

display(irisSeparateFeatures.groupBy("label").count().orderBy("label"))

// COMMAND ----------

// MAGIC %md
// MAGIC Let's build a model that tries to differentiate between the first two classes.

// COMMAND ----------

val irisTwoClass = irisSeparateFeatures.filter($"label" < 2)
display(irisTwoClass.groupBy("label").count().orderBy("label"))

// COMMAND ----------

// MAGIC %md
// MAGIC Next, we'll split our dataset into test and train sets.

// COMMAND ----------

val Array(irisTest, irisTrain) = irisTwoClass.randomSplit(Array(.25, .75), seed=0)
 
// Cache as we'll be using these several times
irisTest.cache()
irisTrain.cache()
 
println(s"Items in test datset: ${irisTest.count}")
println(s"Items in train dataset: ${irisTrain.count}\n\n")

// COMMAND ----------

// MAGIC %md
// MAGIC And now let's build our logistic regression model.  LogisticRegression can be found in [pyspark.ml.classification](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.LogisticRegression) for Python and the [org.apache.spark.ml.classification](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.classification.LogisticRegression) package for Scala.  The ML Guide also has a nice overview of [logistic regression](http://spark.apache.org/docs/latest/ml-classification-regression.html#logistic-regression).
// MAGIC  
// MAGIC Make sure to set the featuresCol to "featuresBucketized", the regParam to 0.0, the labelCol to "label", and the maxIter to 1000.
// MAGIC  
// MAGIC Also, set the pipeline stages to include the two bucketizers, assembler, and logistic regression.

// COMMAND ----------

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
 
val lr = new LogisticRegression()
  .setFeaturesCol("featuresBucketized")
  .setRegParam(0.0)
  .setLabelCol("label")
  .setMaxIter(1000)
 
pipeline.setStages(Array(lengthBucketizer, widthBucketizer, assembler, lr))
 
val pipelineModelLR = pipeline.fit(irisTrain)
 
val irisTestPredictions = pipelineModelLR.transform(irisTest).cache
 
display(irisTestPredictions)

// COMMAND ----------

// TEST
import org.apache.spark.mllib.linalg.DenseVector
assert(irisTestPredictions.select("probability").first.getAs[DenseVector](0).toArray.sum > .99,
       "incorrect build of the logistic model.")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Part 5

// COMMAND ----------

import scala.runtime.ScalaRunTime.stringOf
println(stringOf(pipelineModelLR.stages))
println(pipelineModelLR.stages.last.asInstanceOf[LogisticRegressionModel].weights)

// COMMAND ----------

// MAGIC %md
// MAGIC Leaving our features to range from 0 to 3 means that a value of 2 has twice the impact in our model than a value of 1.  Since these buckets were based on increasing numeric values this is not unreasonable; however, we might want to convert each of these values to a dummy feature that takes on either a 0 or 1 corresponding to whether the value occurs.  This allows the model to measure the impact of the occurrences of the individual values and allows for non-linear relationships.
// MAGIC  
// MAGIC To do this we'll use the `OneHotEncoder` estimator.

// COMMAND ----------

import org.apache.spark.ml.feature.OneHotEncoder
 
val oneHotLength = new OneHotEncoder()
  .setInputCol("lengthFeatures")
  .setOutputCol("lengthOneHot")
 
pipeline.setStages(Array(lengthBucketizer, widthBucketizer, oneHotLength))
 
val irisWithOneHotLength = pipeline.fit(irisTrain).transform(irisTrain)
display(irisWithOneHotLength)

// COMMAND ----------

irisWithOneHotLength.select("lengthOneHot").first

// COMMAND ----------

// MAGIC %md
// MAGIC Create a `OneHotEncoder` for width as well, and combine both encoders together into a `featuresBucketized` column.

// COMMAND ----------

val oneHotWidth = new OneHotEncoder()
  .setInputCol("widthFeatures")
  .setOutputCol("widthOneHot")
 
val assembleOneHot = new VectorAssembler()
  .setInputCols(Array("lengthOneHot", "widthOneHot"))
  .setOutputCol("featuresBucketized")
 
pipeline.setStages(Array(lengthBucketizer, widthBucketizer, oneHotLength, oneHotWidth, assembleOneHot))
 
display(pipeline.fit(irisTrain).transform(irisTrain))

// COMMAND ----------

// MAGIC %md
// MAGIC Create the full `Pipeline` through logistic regression and make predictions on the test data.

// COMMAND ----------

pipeline.setStages(Array(lengthBucketizer, widthBucketizer, oneHotLength, oneHotWidth, assembleOneHot, lr))
 
val pipelineModelLR2 = pipeline.fit(irisTrain)
 
val irisTestPredictions2 = pipelineModelLR2
  .transform(irisTest)
  .cache()
display(irisTestPredictions2)

// COMMAND ----------

// MAGIC %md
// MAGIC What does our new model look like?

// COMMAND ----------

val logisticModel = pipelineModelLR2.stages.last.asInstanceOf[LogisticRegressionModel]
println(logisticModel.intercept)
println(stringOf(logisticModel.weights))

// COMMAND ----------

// MAGIC %md
// MAGIC What about model accuracy?

// COMMAND ----------

import org.apache.spark.sql.DataFrame
 
def modelAccuracy(df: DataFrame) = {
  df
  .select(($"prediction" === $"label").cast("int").alias("correct"))
  .groupBy()
  .avg("correct")
  .first()(0)
}
 
val modelOneAccuracy = modelAccuracy(irisTestPredictions)
val modelTwoAccuracy = modelAccuracy(irisTestPredictions2)
 
println(s"modelOneAccuracy: $modelOneAccuracy")
println(s"modelTwoAccuracy: $modelTwoAccuracy\n\n")

// COMMAND ----------

// MAGIC %md
// MAGIC Or we can use SQL instead.

// COMMAND ----------

irisTestPredictions.registerTempTable("modelOnePredictions")
val sqlResult = sqlContext.sql("select avg(int(prediction == label)) from modelOnePredictions")
display(sqlResult)

// COMMAND ----------

// MAGIC %md
// MAGIC An even better option is to use the tools already built-in to Spark.  The MLlib guide has a lot of information regarding [evaluation metrics](http://spark.apache.org/docs/latest/mllib-evaluation-metrics.html).  For ML, you can find details in the [Python](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.evaluation) and [Scala](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.evaluation.package) APIs.
// MAGIC  
// MAGIC A common metric used for logistic regression is area under the ROC curve (AUC).  We can use the `BinaryClasssificationEvaluator` to obtain the AUC for our two models.  Make sure to set the metric to "areaUnderROC" and that you set the rawPrediction column to "rawPrediction".
// MAGIC  
// MAGIC Recall that `irisTestPredictions` are the test predictions from our first model and `irisTestPredictions2` are the test predictions from our second model.

// COMMAND ----------

// ANSWER
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
 
val binaryEvaluator = new BinaryClassificationEvaluator()
  .setRawPredictionCol("rawPrediction")
  .setMetricName("areaUnderROC")
 
val firstModelTestAUC = binaryEvaluator.evaluate(irisTestPredictions)
val secondModelTestAUC = binaryEvaluator.evaluate(irisTestPredictions2)
 
println(s"First model AUC: $firstModelTestAUC")
println(s"Second model AUC: $secondModelTestAUC")
 
val irisTrainPredictions = pipelineModelLR.transform(irisTrain)
val irisTrainPredictions2 = pipelineModelLR2.transform(irisTrain)
 
val firstModelTrainAUC = binaryEvaluator.evaluate(irisTrainPredictions)
val secondModelTrainAUC = binaryEvaluator.evaluate(irisTrainPredictions2)
 
println(s"\nFirst model training AUC: $firstModelTrainAUC")
println(s"Second model training AUC: $secondModelTrainAUC\n\n")

// COMMAND ----------

assert(firstModelTestAUC > .95, "incorrect firstModelTestAUC")
assert(secondModelTrainAUC > .95, "incorrect secondMOdelTrainAUC")

// COMMAND ----------

// MAGIC %md
// MAGIC **Visualization: ROC curve **
// MAGIC  
// MAGIC We will now visualize how well the model predicts our target.  To do this we generate a plot of the ROC curve.  The ROC curve shows us the trade-off between the false positive rate and true positive rate, as we liberalize the threshold required to predict a positive outcome.  A random model is represented by the dashed line.

// COMMAND ----------

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
 
val metric ="precision"
 
val multiclassEval = new MulticlassClassificationEvaluator()
 
multiclassEval.setMetricName(metric)
println(s"Model one $metric: ${multiclassEval.evaluate(irisTestPredictions)}")
println(s"Model two $metric: ${multiclassEval.evaluate(irisTestPredictions2)}\n")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Using MLlib instead of ML
// MAGIC  
// MAGIC We've been using `ml` transformers, estimators, pipelines, and evaluators.  How can we accomplish the same things with MLlib?

// COMMAND ----------

irisTestPredictions.columns

// COMMAND ----------

irisTestPredictions.take(1)

// COMMAND ----------

// MAGIC %md
// MAGIC Pull the data that we need from our `DataFrame` and create `BinaryClassificationMetrics`.

// COMMAND ----------

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
 
val modelOnePredictionLabel = irisTestPredictions
  .select("rawPrediction", "label")
  .rdd
  .map(r => (r.getAs[DenseVector](0)(1), r.getAs[Double](1)))
 
val modelTwoPredictionLabel = irisTestPredictions2
  .select("rawPrediction", "label")
  .rdd
  .map(r => (r.getAs[DenseVector](0)(1), r.getAs[Double](1)))
 
val metricsOne = new BinaryClassificationMetrics(modelOnePredictionLabel)
val metricsTwo = new BinaryClassificationMetrics(modelTwoPredictionLabel)
 
println(metricsOne.areaUnderROC)
println(metricsTwo.areaUnderROC)

// COMMAND ----------

// MAGIC %md
// MAGIC To build a logistic regression model with MLlib we'll need the data to be an RDD of `LabeledPoints`.  For testing purposes we'll pull out the label and features into a tuple, since we'll want to make predictions directly on the features and not on a `LabeledPoint`.

// COMMAND ----------

import org.apache.spark.mllib.regression.LabeledPoint
 
val irisTrainRDD = irisTrainPredictions
  .select("label", "featuresBucketized")
  .map(r => LabeledPoint(r.getAs[Double](0), r.getAs[Vector](1)))
  .cache
 
val irisTestRDD = irisTestPredictions
  .select("label", "featuresBucketized")
  .map(r => (r.getAs[Double](0), r.getAs[Vector](1)))
  .cache
 
irisTrainRDD.take(2).foreach(println)
irisTestRDD.take(2).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC Now, we can use MLlib's logistic regression on our `RDD` of `LabeledPoints`.  Note that we'll use `LogisticRegressionWithLBFGS` as it tends to converge faster than `LogisticRegressionWithSGD`.

// COMMAND ----------

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
 
val mllibModel = new LogisticRegressionWithLBFGS()
  .run(irisTrainRDD)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's calculate our accuracy using `RDDs`.

// COMMAND ----------

val rddPredictions = mllibModel.predict(irisTestRDD.values)
val predictAndLabels = rddPredictions.zip(irisTestRDD.keys)
 
val mllibAccuracy = predictAndLabels.map( x => if (x._1 == x._2) 1 else 0).mean()
println(s"MLlib model accuracy: $mllibAccuracy")
