// Databricks notebook source exported at Tue, 16 Feb 2016 16:46:55 UTC
// MAGIC %md
// MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

// COMMAND ----------

// MAGIC %md
// MAGIC # MLlib Data Types
// MAGIC  
// MAGIC This notebook explains the machine learning specific data types in Spark.  The focus is on the data types and classes used for generating models.  These include: `DenseVector`, `SparseVector`, `LabeledPoint`, and `Rating`.
// MAGIC  
// MAGIC For reference:
// MAGIC  
// MAGIC The [MLlib Guide](http://spark.apache.org/docs/latest/mllib-guide.html) provides an overview of all aspects of MLlib and [MLlib Guide: Data Types](http://spark.apache.org/docs/latest/mllib-data-types.html) provides a detailed review of data types specific for MLlib
// MAGIC  
// MAGIC After this lab you should understand the differences between `DenseVectors` and `SparseVectors` and be able to create and use `DenseVector`, `SparseVector`, `LabeledPoint`, and `Rating` objects.  You'll also learn where to obtain additional information regarding the APIs and specific class / method functionality.

// COMMAND ----------

// MAGIC %md
// MAGIC  
// MAGIC #### Dense and Sparse
// MAGIC  
// MAGIC MLlib supports both dense and sparse types for vectors and matrices.  We'll focus on vectors as they are most commonly used in MLlib and matrices have poor scaling properties.
// MAGIC  
// MAGIC A dense vector contains an array of values, while a sparse vector stores the size of the vector, an array of indices, and an array of values that correspond to the indices.  A sparse vector saves space by not storing zero values.
// MAGIC  
// MAGIC For example, if we had the dense vector `[2.0, 0.0, 0.0, 3.0, 0.0, 0.0, 0.0]`, we could store that as a sparse vector with size 7, indices as `[0, 3]`, and values as `[2.0, 3.0]`.

// COMMAND ----------

// import data types
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, SparseMatrix, DenseMatrix, Vectors, Matrices}

// COMMAND ----------

// MAGIC %md
// MAGIC  
// MAGIC  
// MAGIC When using Scala it's possible to obtain some details of objects using reflection, but it's recommended to reference the [programming guides](http://spark.apache.org/docs/latest/programming-guide.html), [Scala API](http://spark.apache.org/docs/latest/api/scala/#package), and the Scala [source code](https://github.com/apache/spark) for Spark.

// COMMAND ----------

Vectors.getClass.getMethods mkString "\n"

// COMMAND ----------

// MAGIC %md
// MAGIC #### DenseVector

// COMMAND ----------

// MAGIC %md
// MAGIC  
// MAGIC Spark provides a [DenseVector](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.linalg.DenseVector) class within the package [org.apache.spark.mllib.linalg](https://spark.apache.org/docs/1.5.0/api/scala/index.html#org.apache.spark.mllib.linalg.package).  `DenseVector` is used to store arrays of values for use in Spark.
// MAGIC  
// MAGIC `DenseVector` objects exist locally and are not inherently distributed.  `DenseVector` objects can be used in the distributed setting by including them in `RDDs` or `DataFrames`.
// MAGIC  
// MAGIC You can create a dense vector by using the [Vectors](https://spark.apache.org/docs/1.5.0/api/scala/index.html#org.apache.spark.mllib.linalg.Vectors$) object and calling `Vectors.dense`.  The `Vectors` object also contains a method for creating `SparseVectors`.

// COMMAND ----------

// Create a DenseVector using Vectors
val denseVector = Vectors.dense(Array(1.0, 2.0, 3.0))
 
println(s"denseVector.getClass: ${denseVector.getClass}")
println(s"denseVector: $denseVector")

// COMMAND ----------

// MAGIC %md
// MAGIC ** Norm **
// MAGIC  
// MAGIC We can calculate the norm of a vector using `Vectors.norm`.  The norm calculation is:
// MAGIC  
// MAGIC   \\[ ||x|| _p = \bigg( \sum_i^n |x_i|^p \bigg)^{1/p} \\]
// MAGIC  
// MAGIC  
// MAGIC  
// MAGIC Sometimes we'll want to normalize our features before training a model.  Later on we'll use the `ml` library to perform this normalization using a transformer.

// COMMAND ----------

Vectors.norm(denseVector, 2)

// COMMAND ----------

// MAGIC %md
// MAGIC Sometimes we'll want to treat a vector as an array.  We can convert both sparse and dense vectors to arrays by calling the `toArray` method on the vector.

// COMMAND ----------

val denseArray = denseVector.toArray

// COMMAND ----------

// MAGIC %md
// MAGIC #### SparseVector

// COMMAND ----------

// MAGIC %md
// MAGIC Let's create a `SparseVector` using [Vectors.sparse](https://spark.apache.org/docs/1.5.0/api/scala/index.html#org.apache.spark.mllib.linalg.Vectors$)

// COMMAND ----------

// Using asInstanceOf so we can access its SparseVector specific attributes later
val sparseVector = Vectors.sparse(10, Array(2, 7), Array(1.0, 5.0)).asInstanceOf[SparseVector]

// COMMAND ----------

// MAGIC %md
// MAGIC  
// MAGIC Let's take a look at what fields and methods are available with a `SparseVector`.  Here are links to the [Python](https://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.linalg.SparseVector) and [Scala](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.mllib.linalg.SparseVector) APIs for `SparseVector`.

// COMMAND ----------

// Note that this is the object
println(SparseVector.getClass.getMethods mkString "\n")
println(s"\n${SparseVector.getClass}")

// COMMAND ----------

// This is an instance of the class
println(sparseVector.getClass.getMethods mkString "\n")
 
println(s"\n${SparseVector.getClass}")

// COMMAND ----------

sparseVector.indices

// COMMAND ----------

import scala.runtime.ScalaRunTime.stringOf
println(s"sparseVector.size: ${sparseVector.size}")
println(s"sparseVector.size.getClass: ${sparseVector.size.getClass}")
 
println(s"\nsparseVector.indices: ${stringOf(sparseVector.indices)}")
println(s"sparseVector.indices.getClass: ${sparseVector.indices(0).getClass}")
 
println(s"\nsparseVector.values: ${stringOf(sparseVector.values)}")
println(s"sparseVector.values.getClass: ${sparseVector.values(0).getClass}\n\n")

// COMMAND ----------

Vectors.norm(sparseVector, 2)

// COMMAND ----------

// MAGIC %md
// MAGIC #### LabeledPoint

// COMMAND ----------

// MAGIC %md
// MAGIC  
// MAGIC In MLlib, labeled training instances are stored using the [LabeledPoint](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.mllib.regression.LabeledPoint) object.  Note that the features and label for a `LabeledPoint` are stored in the `features` and `label` attribute of the object.

// COMMAND ----------

import org.apache.spark.mllib.regression.LabeledPoint

// COMMAND ----------

val labeledPoint = LabeledPoint(1992, Vectors.dense(Array(3.0, 5.5, 10.0)))
println(s"labeledPoint: $labeledPoint")
 
println(s"\nlabeledPoint.features: ${stringOf(labeledPoint.features)}")
println(s"labeledPoint.features.getClass: ${labeledPoint.features.getClass}")
 
println(s"\nlabeledPoint.label: ${labeledPoint.label}")
println(s"labeledPoint.label.getClass: ${labeledPoint.label.getClass}\n\n")

// COMMAND ----------

val labeledPointSparse = LabeledPoint(1992, Vectors.sparse(10, Array(0, 1, 2), Array(3.0, 5.5, 10.0)))
println(s"labeledPointSparse: $labeledPointSparse")
 
println(s"\nlabeledPointSparse.features: ${stringOf(labeledPointSparse.features)}")
println(s"labeledPointSparse.features.getClass: ${labeledPointSparse.features.getClass}")
 
println(s"\nlabeledPointSparse.label: ${labeledPointSparse.label}")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Rating

// COMMAND ----------

// MAGIC %md
// MAGIC When performing collaborative filtering we aren't working with vectors or labeled points, so we need another type of object to capture the relationship between users, products, and ratings.  This is represented by a `Rating` which can be found in the [Python](http://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.recommendation.Rating) and [Scala](https://spark.apache.org/docs/1.5.0/api/scala/index.html#org.apache.spark.mllib.recommendation.Rating) APIs.

// COMMAND ----------

import org.apache.spark.mllib.recommendation.Rating

// COMMAND ----------

val rating = Rating(4, 10, 2.0)
println(s"rating: $rating")
println(s"rating.user: ${rating.user}")
println(s"rating.product: ${rating.product}")
println(s"rating.rating: ${rating.rating}\n\n")

// COMMAND ----------

// MAGIC %md
// MAGIC #### DataFrames
// MAGIC  
// MAGIC When using Spark's ML library rather than MLlib you'll be working with `DataFrames` instead of `RDDs`.  In this section we'll show how you can create a `DataFrame` using MLlib datatypes.

// COMMAND ----------

// MAGIC %md
// MAGIC When using Scala we can create a case class to capture the structure of a row of data.  Below, we'll create a case class for an address.  We can use case classes to generate `DataFrames`.

// COMMAND ----------

case class Address(city: String, state: String)
val address = Address("Boulder", "CO")
 
println(s"address: $address")
println(s"address.city: ${address.city}")
println(s"address.state: ${address.state}\n\n")

// COMMAND ----------

display(sqlContext.createDataFrame(Seq(Address("Boulder", "CO"), Address("New York", "NY"))))

// COMMAND ----------

// MAGIC %md
// MAGIC Let's create a `DataFrame` with a couple of rows where the first column is the label and the second is the features.

// COMMAND ----------

import org.apache.spark.mllib.linalg.Vector
 
case class LabelAndFeatures(label: Double, features: Vector)
val row1 = LabelAndFeatures(10, Vectors.dense(Array(1.0, 2.0)))
val row2 = LabelAndFeatures(20, Vectors.dense(Array(1.5, 2.2)))
 
val df = sqlContext.createDataFrame(Seq(row1, row2))
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Exercises

// COMMAND ----------

// MAGIC %md
// MAGIC Create a `DenseVector` with the values 1.5, 2.5, 3.0 (in that order).

// COMMAND ----------

// ANSWER
val denseVec = Vectors.dense(Array(1.5, 2.5, 3.0))

// COMMAND ----------

// TEST
assert(denseVec == new DenseVector(Array(1.5, 2.5, 3.0)), "incorrect value for denseVec")

// COMMAND ----------

// MAGIC %md
// MAGIC Create a `LabeledPoint` with a label equal to 10.0 and features equal to `denseVec`

// COMMAND ----------

// ANSWER
val labeledP = LabeledPoint(10.0, denseVec)

// COMMAND ----------

// TEST
assert(labeledP.toString == "(10.0,[1.5,2.5,3.0])", "incorrect value for labeledP")

// COMMAND ----------

// MAGIC %md
// MAGIC ** Challenge Question [Intentionally Hard]**
// MAGIC  
// MAGIC Create a `udf` that pulls the first element out of a column that contains `DenseVectors`.

// COMMAND ----------

// ANSWER
import org.apache.spark.sql.functions.udf
 
val firstElement = udf { v: Vector => v(0) }
 
val df2 = df.select(firstElement($"features").as("first"))
df2.show()

// COMMAND ----------

// TEST
assert(df2.rdd.map(_(0)).collect().deep == Array(1.0, 1.5).deep, "incorrect implementation of firstElement")
