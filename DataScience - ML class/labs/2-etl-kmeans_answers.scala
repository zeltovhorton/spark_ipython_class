// Databricks notebook source exported at Tue, 16 Feb 2016 18:11:29 UTC
// MAGIC %md
// MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

// COMMAND ----------

// MAGIC %md
// MAGIC # ETL and K-Means
// MAGIC  
// MAGIC This lab will demonstrate loading data from a file, transforming that data into a form usable with the ML and MLlib libraries, and building a k-means clustering model using both ML and MLlib.
// MAGIC  
// MAGIC Upon completing this lab you should understand how to read from and write to files in Spark, convert between `RDDs` and `DataFrames`, and build a model using both the ML and MLlib APIs.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Loading the data
// MAGIC  
// MAGIC First, we need to load data into Spark.  We'll use a built-in utility to load a [libSVM file](http://www.csie.ntu.edu.tw/~cjlin/libsvm/faq.html), which is stored in an S3 bucket on AWS.  We'll use `MLUtils.loadLibSVMFile` to load our file.  Here are the [Python](http://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.util.MLUtils.loadLibSVMFile) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.util.MLUtils$) APIs.

// COMMAND ----------

import org.apache.spark.mllib.util.MLUtils
 
val baseDir = "/mnt/ml-class/"
val irisPath = baseDir + "iris.scale"
val irisRDD = MLUtils.loadLibSVMFile(sc, irisPath, 4, 20).cache
 
// We get back an RDD of LabeledPoints.  Note that the libSVM format uses SparseVectors.
irisRDD.take(5)

// COMMAND ----------

// MAGIC %md
// MAGIC What if we wanted to see the first few lines of the libSVM file to see what the format looks like?

// COMMAND ----------

sc.textFile(irisPath).take(5).mkString("\n")

// COMMAND ----------

// MAGIC %md
// MAGIC How is this data stored across partitions?

// COMMAND ----------

println(s"number of partitions: ${irisRDD.partitions.size}")
val elementsPerPart = irisRDD
  .mapPartitionsWithIndex( (i, x) => Iterator((i, x.toArray.size)))
  .collect()
irisRDD.glom().first.mkString("\n")

// COMMAND ----------

// MAGIC %md
// MAGIC Let's convert this `RDD` of `LabeledPoints` to a `DataFrame`

// COMMAND ----------

val irisDF = irisRDD.toDF()
irisDF.take(5).mkString("\n")

// COMMAND ----------

irisDF.show(truncate=false)

// COMMAND ----------

display(irisDF)

// COMMAND ----------

println(irisDF.schema + "\n")
irisDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC Why were we able to convert directly from a `LabeledPoint` to a `Row`?

// COMMAND ----------

// MAGIC %md
// MAGIC Python function calls for converting a RDD into a DataFrame
// MAGIC  
// MAGIC [createDataFrame](https://github.com/apache/spark/blob/3a11e50e21ececbec9708eb487b08196f195cd87/python/pyspark/sql/context.py#L342)
// MAGIC  
// MAGIC --> [_createFromRDD](https://github.com/apache/spark/blob/3a11e50e21ececbec9708eb487b08196f195cd87/python/pyspark/sql/context.py#L280)
// MAGIC  
// MAGIC ----> [_inferSchema](https://github.com/apache/spark/blob/3a11e50e21ececbec9708eb487b08196f195cd87/python/pyspark/sql/context.py#L221)
// MAGIC  
// MAGIC ------> [_infer_schema](https://github.com/apache/spark/blob/3a11e50e21ececbec9708eb487b08196f195cd87/python/pyspark/sql/types.py#L813)
// MAGIC  
// MAGIC --> [back to _createFromRDD](https://github.com/apache/spark/blob/3a11e50e21ececbec9708eb487b08196f195cd87/python/pyspark/sql/context.py#L304)
// MAGIC  
// MAGIC ----> [toInternal](https://github.com/apache/spark/blob/3a11e50e21ececbec9708eb487b08196f195cd87/python/pyspark/sql/types.py#L533)
// MAGIC  
// MAGIC [back to createDataFrame](https://github.com/apache/spark/blob/3a11e50e21ececbec9708eb487b08196f195cd87/python/pyspark/sql/context.py#L404)

// COMMAND ----------

case class Person(name: String, age: Int)

// COMMAND ----------

val personDF = sqlContext.createDataFrame(Seq(Person("Bob", 28), Person("Julie", 25)))
display(personDF)

// COMMAND ----------

// Show the schema that was inferred
println(personDF.schema)
personDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC #### Transform the data
// MAGIC  
// MAGIC If you look at the data you'll notice that there are three values for label: 1, 2, and 3.  Spark's machine learning algorithms expect a 0 indexed target variable, so we'll want to adjust those labels.  This transformation is a simple expression where we'll subtract one from our `label` column.
// MAGIC  
// MAGIC For help, reference the SQL Programming Guide portion on [dataframe-operations](http://spark.apache.org/docs/latest/sql-programming-guide.html#dataframe-operations) or the Spark SQL [Python](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.package) APIs.  `select`, `col`, and `alias` can be used to accomplish this.
// MAGIC  
// MAGIC The resulting `DataFrame` should have two columns: one named `features` and another named `label`.

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.functions.col
 
val irisDFZeroIndex = irisDF.select($"features", (col("label") - 1).alias("label"))
display(irisDFZeroIndex)

// COMMAND ----------

assert(irisDFZeroIndex.select("label").map(_(0)).take(3).deep == Array(0, 0, 0).deep,
       "incorrect value for irisDFZeroIndex")

// COMMAND ----------

// You can use $ as a shortcut for a column.  Using $ creates a ColumnName which extends Column, so it can be used where Columns are expected
$"features"

// COMMAND ----------

// MAGIC %md
// MAGIC You'll also notice that we have four values for features and that those values are stored as a `SparseVector`.  We'll reduce those down to two values (for visualization purposes) and convert them to a `DenseVector`.  To do that we'll need to create a `udf` and apply it to our dataset.  Here's a `udf` reference for [Python](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.udf) and for [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.UserDefinedFunction).
// MAGIC  
// MAGIC Note that you can call the `toArray` method on a `SparseVector` to obtain an array, and you can convert an array into a `DenseVector` using the `Vectors.dense` method.

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.functions.udf
import org.apache.spark.mllib.linalg.{Vectors, Vector}
 
// Take the first two values from a SparseVector and convert them to a DenseVector
val firstTwoFeatures = udf { (vector: Vector) => Vectors.dense(Array(vector(0), vector(1))) }
 
val irisTwoFeaturesUDF = irisDFZeroIndex.select(firstTwoFeatures($"features").as("features"),  $"label").cache()
display(irisTwoFeaturesUDF)

// COMMAND ----------

// TEST
assert(irisTwoFeaturesUDF.first.toString() == "[[-0.555556,0.25],0.0]",
       "incorrect definition of firstTwoFeatures")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Part 2

// COMMAND ----------

import org.apache.spark.ml.feature.VectorSlicer
val vs = new VectorSlicer()
  .setInputCol("features")
  .setOutputCol("twoFeatures")
  .setIndices(Array(0, 1))
val irisTwoFeaturesSlicer = vs.transform(irisDFZeroIndex)
  .select($"twoFeatures".as("features"), $"label")

// COMMAND ----------

// VectorSlicer returns a SparseVector
irisTwoFeaturesSlicer.first().getAs[Vector](0)

// COMMAND ----------

// Let's create a UDF to generate a DenseVector instead
val toDense = udf { (vector: Vector) => vector.toDense }
 
val irisTwoFeatures = irisTwoFeaturesSlicer.select(toDense($"features").as("features"), $"label")
irisTwoFeatures.first.getAs[Vector](0)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's view our `irisTwoFeatures` `DataFrame`.

// COMMAND ----------

irisTwoFeatures.take(5).mkString("\n")

// COMMAND ----------

display(irisTwoFeatures)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Saving our DataFrame

// COMMAND ----------

// MAGIC %md
// MAGIC We'll be using parquet files to save our data.  More information about the parquet file format can be found on [parquet.apache.org](https://parquet.apache.org/documentation/latest/).

// COMMAND ----------

import scala.util.Random
val id = Random.nextLong.toString
irisTwoFeatures.write.mode("overwrite").parquet(s"/tmp/$id/irisTwoFeatures.parquet")

// COMMAND ----------

// MAGIC %md
// MAGIC Note that we'll get a part file for each partition and that these files are compressed.

// COMMAND ----------

//display(dbutils.fs.ls(baseDir + "irisTwoFeatures.parquet"))
display(dbutils.fs.ls(s"/tmp/$id/irisTwoFeatures.parquet"))

// COMMAND ----------

irisDFZeroIndex.write.mode("overwrite").parquet(s"/tmp/$id/irisFourFeatures.parquet")

// COMMAND ----------

// MAGIC %md
// MAGIC #### K-Means

// COMMAND ----------

// MAGIC %md
// MAGIC Now we'll build a k-means model using our two features and inspect the class hierarchy.
// MAGIC  
// MAGIC We'll build the k-means model using `KMeans`, an `ml` `Estimator`.  Details can be found in the [Python](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.clustering) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.clustering.package) APIs.  Also, examples that use [PCA](http://spark.apache.org/docs/latest/ml-features.html#pca) and  [logistic regression](http://spark.apache.org/docs/latest/ml-guide.html#example-estimator-transformer-and-param) can be found in the ML Programming Guide.
// MAGIC  
// MAGIC Make sure to work with the `irisTwoFeatures` `DataFrame`.

// COMMAND ----------

import org.apache.spark.ml.clustering.KMeans
 
// Create a KMeans Estimator and set k=3, seed=5, maxIter=20, initSteps=1
val kmeans = new KMeans()
  .setK(3)
  .setSeed(5)
  .setMaxIter(20)
  .setInitSteps(1)
 
// Call fit on the estimator and pass in our DataFrame
val model = kmeans.fit(irisTwoFeatures)
 
// Obtain the clusterCenters from the KMeansModel
val centers = model.clusterCenters
 
// Use the model to transform the DataFrame by adding cluster predictions
val transformed = model.transform(irisTwoFeatures)

// COMMAND ----------

// TEST
import org.apache.spark.mllib.linalg.Vectors
 
assert(math.round(centers(0)(0)* 1000) == 351,
       "incorrect centers.  check your params.")
assert(transformed.select("prediction").map(_(0)).take(4).deep == Array(1,1,1,1).deep,
       "incorrect predictions")

// COMMAND ----------

// MAGIC %md
// MAGIC ## PART 3

// COMMAND ----------

// MAGIC %md
// MAGIC From the class hierarchy it is clear that `KMeans` is an `Estimator` while `KMeansModel` is a `Transformer`.

// COMMAND ----------

import scala.reflect.runtime.universe._
 
def typesOf[T : TypeTag](v: T): List[Type] =
  typeOf[T].baseClasses.map(typeOf[T].baseType)
 
println("*** KMeans instance base classes ***")
typesOf(kmeans).foreach(println)
 
println("\n\n*** KMeansModel base classes ***")
typesOf(model).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's print the three centroids of our model

// COMMAND ----------

centers.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC Note that our predicted cluster is appended, as a column, to our input `DataFrame`.  Here it would be desirable to see consistency between label and prediction.  These don't need to be the same number but if label 0 is usually predicted to be cluster 1 that would indicate that our unsupervised learning is naturally grouping the data into species.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Using MLlib instead of ML

// COMMAND ----------

// MAGIC %md
// MAGIC First, convert our `DataFrame` into an `RDD`.

// COMMAND ----------

// Note that .rdd is not necessary, but is here to illustrate that we are working with an RDD
val irisTwoFeaturesRDD = irisTwoFeatures
  .rdd
  .map(r => (r.getAs[Double](1), r.getAs[Vector](0)))
irisTwoFeaturesRDD.take(2)

// COMMAND ----------

import org.apache.spark.sql.Row
// Or alternatively, we could use a pattern match on the Row case class
val irisTwoFeaturesRDD = irisTwoFeatures
  .rdd
  .map { case Row(feature: Vector, label: Double) => (label, feature) }
irisTwoFeaturesRDD.take(2)

// COMMAND ----------

// MAGIC %md
// MAGIC Then import MLlib's `KMeans` as `MLlibKMeans` to differentiate it from `ml.KMeans`

// COMMAND ----------

import org.apache.spark.mllib.clustering.{KMeans => MLlibKMeans}

// COMMAND ----------

// MAGIC %md
// MAGIC Finally, let's build our k-means model.  Here are the relevant [Python](http://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.clustering.KMeans) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.clustering.KMeans) APIs.
// MAGIC  
// MAGIC Make sure to set `k` to 3, `maxIterations` to 20, `seed` to 5, and `initializationSteps` to 1.  Also, note that we returned an `RDD` with (label, feature) tuples.  You'll just need the features, which you can obtain by calling `.values()` on `irisTwoFeaturesRDD`.

// COMMAND ----------

// ANSWER
import runtime.ScalaRunTime.stringOf
 
val mllibKMeans = new MLlibKMeans()
  .setK(3)
  .setMaxIterations(20)
  .setSeed(5)
  .setInitializationSteps(1)
  .run(irisTwoFeaturesRDD.values)
 
println(s"mllib: ${stringOf(mllibKMeans.clusterCenters)}")
println(s"ml:    ${stringOf(centers)}")

// COMMAND ----------

// TEST
assert(math.round(mllibKMeans.clusterCenters(0)(0) * 1000) == math.round(centers(0)(0) * 1000),
       "Your mllib and ml models don't match")

// COMMAND ----------

// MAGIC %md
// MAGIC Now that we have an `mllibKMeans` model how do we generate predictions and compare those to our labels?

// COMMAND ----------

val predictionsRDD = mllibKMeans.predict(irisTwoFeaturesRDD.values)
predictionsRDD.take(5)

// COMMAND ----------

// MAGIC %md
// MAGIC We'll use `zip` to combine the feature and prediction RDDs together.  Note that zip assumes that the RDDs have the same number of partitions and that each partition has the same number of elements.  This is true here as our predictions were the result of a `map` operation on the feature RDD.

// COMMAND ----------

val combinedRDD = irisTwoFeaturesRDD.zip(predictionsRDD)
combinedRDD.take(5).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's compare this to the result from `ml`.

// COMMAND ----------

display(transformed)

// COMMAND ----------

// MAGIC %md
// MAGIC #### How do the `ml` and `mllib` implementations differ?

// COMMAND ----------

// MAGIC %md
// MAGIC The `ml` version of k-means is just a wrapper to MLlib's implementation.  Let's take a look here:
// MAGIC [org.apache.spark.ml.clustering.KMeans source](https://github.com/apache/spark/blob/e1e77b22b3b577909a12c3aa898eb53be02267fd/mllib/src/main/scala/org/apache/spark/ml/clustering/KMeans.scala#L192).
// MAGIC  
// MAGIC How is $ being used in this function? `Param` [source code](https://github.com/apache/spark/blob/2b574f52d7bf51b1fe2a73086a3735b633e9083f/mllib/src/main/scala/org/apache/spark/ml/param/params.scala#L643) has the answer.
// MAGIC  
// MAGIC Which is different than $'s usage for SQL columns where it is a [string interpolator that returns a ColumnName](https://github.com/apache/spark/blob/3d683a139b333456a6bd8801ac5f113d1ac3fd18/sql/core/src/main/scala/org/apache/spark/sql/SQLContext.scala#L386)

// COMMAND ----------

// So $ followed by a string is treated as a custom string interpolator that creates a ColumnName
val num = 10
$"column$num"
