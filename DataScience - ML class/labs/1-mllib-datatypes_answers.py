# Databricks notebook source exported at Tue, 16 Feb 2016 16:46:28 UTC
# MAGIC %md
# MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC # MLlib Data Types
# MAGIC  
# MAGIC This notebook explains the machine learning specific data types in Spark.  The focus is on the data types and classes used for generating models.  These include: `DenseVector`, `SparseVector`, `LabeledPoint`, and `Rating`.
# MAGIC  
# MAGIC For reference:
# MAGIC  
# MAGIC The [MLlib Guide](http://spark.apache.org/docs/latest/mllib-guide.html) provides an overview of all aspects of MLlib and [MLlib Guide: Data Types](http://spark.apache.org/docs/latest/mllib-data-types.html) provides a detailed review of data types specific for MLlib
# MAGIC  
# MAGIC After this lab you should understand the differences between `DenseVectors` and `SparseVectors` and be able to create and use `DenseVector`, `SparseVector`, `LabeledPoint`, and `Rating` objects.  You'll also learn where to obtain additional information regarding the APIs and specific class / method functionality.

# COMMAND ----------

from pyspark.mllib import linalg
dir(linalg)

# COMMAND ----------

help(linalg)

# COMMAND ----------

# MAGIC %md
# MAGIC  
# MAGIC #### Dense and Sparse
# MAGIC  
# MAGIC MLlib supports both dense and sparse types for vectors and matrices.  We'll focus on vectors as they are most commonly used in MLlib and matrices have poor scaling properties.
# MAGIC  
# MAGIC A dense vector contains an array of values, while a sparse vector stores the size of the vector, an array of indices, and an array of values that correspond to the indices.  A sparse vector saves space by not storing zero values.
# MAGIC  
# MAGIC For example, if we had the dense vector `[2.0, 0.0, 0.0, 3.0, 0.0, 0.0, 0.0]`, we could store that as a sparse vector with size 7, indices as `[0, 3]`, and values as `[2.0, 3.0]`.

# COMMAND ----------

# import data types
from pyspark.mllib.linalg import DenseVector, SparseVector, SparseMatrix, DenseMatrix, Vectors, Matrices

# COMMAND ----------

# MAGIC %md
# MAGIC A great way to get help when using Python is to use the help function on an object or method.  If help isn't sufficient, other places to look include the [programming guides](http://spark.apache.org/docs/latest/programming-guide.html), the [Python API](http://spark.apache.org/docs/latest/api/python/index.html), and directly in the [source code](https://github.com/apache/spark/tree/master/python/pyspark) for PySpark.

# COMMAND ----------

help(Vectors.dense)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DenseVector

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark provides a [DenseVector](https://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.linalg.DenseVector) class within the module [pyspark.mllib.linalg](https://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#module-pyspark.mllib.linalg).  `DenseVector` is used to store arrays of values for use in PySpark.  `DenseVector` actually stores values in a [NumPy array](http://docs.scipy.org/doc/numpy/reference/generated/numpy.array.html) and delegates calculations to that object.  You can create a new `DenseVector` using `DenseVector()` and passing in an NumPy array or a Python list.
# MAGIC  
# MAGIC `DenseVector` implements several functions, such as `DenseVector.dot()` and `DenseVector.norm()`.
# MAGIC  
# MAGIC Note that `DenseVector` stores all values as `np.float64`, so even if you pass in a NumPy array of integers, the resulting `DenseVector` will contain floating-point numbers. Also, `DenseVector` objects exist locally and are not inherently distributed.  `DenseVector` objects can be used in the distributed setting by including them in `RDDs` or `DataFrames`.
# MAGIC  
# MAGIC You can create a dense vector by using the [Vectors](http://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.linalg.Vectors) object and calling `Vectors.dense`.  The `Vectors` object also contains a method for creating `SparseVectors`.

# COMMAND ----------

# Create a DenseVector using Vectors
denseVector = Vectors.dense([1, 2, 3])

print 'type(denseVector): {0}'.format(type(denseVector))
print '\ndenseVector: {0}'.format(denseVector)

# COMMAND ----------

# MAGIC %md
# MAGIC ** Dot product **
# MAGIC  
# MAGIC We can calculate the dot product of two vectors, or a vector and itself, by using `DenseVector.dot()`.  Note that the dot product is equivalent to performing element-wise multiplication and then summing the result.
# MAGIC  
# MAGIC Below, you'll find the calculation for the dot product of two vectors, where each vector has length \\( n \\):
# MAGIC  
# MAGIC \\[ w \cdot x = \sum_{i=1}^n w_i x_i \\]
# MAGIC  
# MAGIC Note that you may also see \\( w \cdot x \\) represented as \\( w^\top x \\)

# COMMAND ----------

denseVector.dot(denseVector)

# COMMAND ----------

# MAGIC %md
# MAGIC ** Norm **
# MAGIC  
# MAGIC We can calculate the norm of a vector using `Vectors.norm`.  The norm calculation is:
# MAGIC  
# MAGIC   \\[ ||x|| _p = \bigg( \sum_i^n |x_i|^p \bigg)^{1/p} \\]
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC Sometimes we'll want to normalize our features before training a model.  Later on we'll use the `ml` library to perform this normalization using a transformer.

# COMMAND ----------

Vectors.norm(denseVector, 2)

# COMMAND ----------

# MAGIC %md
# MAGIC In Python, `DenseVector` operations are delegated to an underlying NumPy array so we can perform multiplication, addition, division, etc.

# COMMAND ----------

denseVector * denseVector

# COMMAND ----------

5 + denseVector

# COMMAND ----------

# MAGIC %md
# MAGIC Sometimes we'll want to treat a vector as an array.  We can convert both sparse and dense vectors to arrays by calling the `toArray` method on the vector.

# COMMAND ----------

denseArray = denseVector.toArray()
print denseArray


# COMMAND ----------

print 'type(denseArray): {0}'.format(type(denseArray))

# COMMAND ----------

# MAGIC %md
# MAGIC #### SparseVector

# COMMAND ----------

help(Vectors.sparse)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's create a `SparseVector` using [Vectors.sparse](http://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.linalg.Vectors.sparse).

# COMMAND ----------

sparseVector = Vectors.sparse(10, [2, 7], [1.0, 5.0])
print 'type(sparseVector): {0}'.format(type(sparseVector))
print '\nsparseVector: {0}'.format(sparseVector)

# COMMAND ----------

# MAGIC %md
# MAGIC  
# MAGIC Let's take a look at what fields and methods are available with a `SparseVector`.  Here are links to the [Python](https://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.linalg.SparseVector) and [Scala](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.mllib.linalg.SparseVector) APIs for `SparseVector`.

# COMMAND ----------

help(SparseVector)

# COMMAND ----------

dir(SparseVector)

# COMMAND ----------

dir(sparseVector)

# COMMAND ----------

# Show the difference between the two
set(dir(sparseVector)) - set(dir(SparseVector))

# COMMAND ----------

# inspect is a handy tool for seeing the Python source code
import inspect
print inspect.getsource(SparseVector)

# COMMAND ----------

print 'sparseVector.size: {0}'.format(sparseVector.size)
print 'type(sparseVector.size):{0}'.format(type(sparseVector.size))

print '\nsparseVector.indices: {0}'.format(sparseVector.indices)
print 'type(sparseVector.indices):{0}'.format(type(sparseVector.indices))
print 'type(sparseVector.indices[0]):{0}'.format(type(sparseVector.indices[0]))

print '\nsparseVector.values: {0}'.format(sparseVector.values)
print 'type(sparseVector.values):{0}'.format(type(sparseVector.values))
print 'type(sparseVector.values[0]):{0}'.format(type(sparseVector.values[0]))

# COMMAND ----------

# MAGIC %md
# MAGIC Don't try to set these values directly.  If you use the wrong type, hard-to-debug errors will occur when Spark attempts to use the `SparseVector`. Create a new `SparseVector` using `Vectors.sparse`

# COMMAND ----------

set(dir(DenseVector)) - set(dir(SparseVector))

# COMMAND ----------

denseVector + denseVector

# COMMAND ----------

try:
    sparseVector + sparseVector
except TypeError as e:
    print e

# COMMAND ----------

sparseVector.dot(sparseVector)

# COMMAND ----------

sparseVector.norm(2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### LabeledPoint

# COMMAND ----------

# MAGIC %md
# MAGIC  
# MAGIC In MLlib, labeled training instances are stored using the [LabeledPoint](https://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.regression.LabeledPoint) object.  Note that the features and label for a `LabeledPoint` are stored in the `features` and `label` attribute of the object.

# COMMAND ----------

from pyspark.mllib.regression import LabeledPoint
help(LabeledPoint)

# COMMAND ----------

labeledPoint = LabeledPoint(1992, [3.0, 5.5, 10.0])
print 'labeledPoint: {0}'.format(labeledPoint)

print '\nlabeledPoint.features: {0}'.format(labeledPoint.features)
# Notice that feaures are being stored as a DenseVector
print 'type(labeledPoint.features): {0}'.format(type(labeledPoint.features))

print '\nlabeledPoint.label: {0}'.format(labeledPoint.label)
print 'type(labeledPoint.label): {0}'.format(type(labeledPoint.label))


# COMMAND ----------

# View the differences between the class and an instantiated instance
set(dir(labeledPoint)) - set(dir(LabeledPoint))

# COMMAND ----------

labeledPointSparse = LabeledPoint(1992, Vectors.sparse(10, {0: 3.0, 1:5.5, 2: 10.0}))
print 'labeledPointSparse: {0}'.format(labeledPointSparse)

print '\nlabeledPoint.featuresSparse: {0}'.format(labeledPointSparse.features)
print 'type(labeledPointSparse.features): {0}'.format(type(labeledPointSparse.features))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rating

# COMMAND ----------

from pyspark.mllib.recommendation import Rating
help(Rating)

# COMMAND ----------

# MAGIC %md
# MAGIC When performing collaborative filtering we aren't working with vectors or labeled points, so we need another type of object to capture the relationship between users, products, and ratings.  This is represented by a `Rating` which can be found in the [Python](http://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.recommendation.Rating) and [Scala](https://spark.apache.org/docs/1.5.0/api/scala/index.html#org.apache.spark.mllib.recommendation.Rating) APIs.

# COMMAND ----------

print inspect.getsource(Rating)

# COMMAND ----------

rating = Rating(4, 10, 2.0)

print 'rating: {0}'.format(rating)
# Note that we can pull out the fields using a dot notation or indexing
print rating.user, rating[0]

# COMMAND ----------

# MAGIC %md
# MAGIC #### DataFrames
# MAGIC  
# MAGIC When using Spark's ML library rather than MLlib you'll be working with `DataFrames` instead of `RDDs`.  In this section we'll show how you can create a `DataFrame` using MLlib datatypes.

# COMMAND ----------

# MAGIC %md
# MAGIC Above we saw that `Rating` is a `namedtuple`.  `namedtuples` are useful when working with `DataFrames`.  We'll explore how they work below and use them to create a `DataFrame`.

# COMMAND ----------

from collections import namedtuple
help(namedtuple)

# COMMAND ----------

Address = namedtuple('Address', ['city', 'state'])
address = Address('Boulder', 'CO')
print 'address: {0}'.format(address)

print '\naddress.city: {0}'.format(address.city)
print 'address[0]: {0}'.format(address[0])

print '\naddress.State: {0}'.format(address.state)
print 'address[1]: {0}'.format(address[1])


# COMMAND ----------

display(sqlContext.createDataFrame([Address('Boulder', 'CO'), Address('New York', 'NY')]))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's create a `DataFrame` with a couple of rows where the first column is the label and the second is the features.

# COMMAND ----------

LabelAndFeatures = namedtuple('LabelAndFeatures', ['label', 'features'])
row1 = LabelAndFeatures(10, Vectors.dense([1.0, 2.0]))
row2 = LabelAndFeatures(20, Vectors.dense([1.5, 2.2]))

df = sqlContext.createDataFrame([row1, row2])
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC Create a `DenseVector` with the values 1.5, 2.5, 3.0 (in that order).

# COMMAND ----------

# ANSWER
denseVec = Vectors.dense([1.5, 2.5, 3.0])

# COMMAND ----------

# TEST
from test_helper import Test
Test.assertEquals(denseVec, DenseVector([1.5, 2.5, 3.0]), 'incorrect value for denseVec')

# COMMAND ----------

# MAGIC %md
# MAGIC Create a `LabeledPoint` with a label equal to 10.0 and features equal to `denseVec`

# COMMAND ----------

# ANSWER
labeledP = LabeledPoint(10.0, denseVec)

# COMMAND ----------

# TEST
Test.assertEquals(str(labeledP), '(10.0,[1.5,2.5,3.0])', 'incorrect value for labeledP')

# COMMAND ----------

# MAGIC %md
# MAGIC ** Challenge Question [Intentionally Hard]**
# MAGIC  
# MAGIC Create a `udf` that pulls the first element out of a column that contains `DenseVectors`.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

firstElement = udf(lambda v: float(v[0]), DoubleType())

df2 = df.select(firstElement('features').alias('first'))
df2.show()

# COMMAND ----------

# TEST
Test.assertEquals(df2.rdd.map(lambda r: r[0]).collect(), [1.0, 1.5], 'incorrect implementation of firstElement')
