# Databricks notebook source exported at Tue, 16 Feb 2016 18:10:42 UTC
# MAGIC %md
# MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC # ETL and K-Means
# MAGIC  
# MAGIC This lab will demonstrate loading data from a file, transforming that data into a form usable with the ML and MLlib libraries, and building a k-means clustering model using both ML and MLlib.
# MAGIC  
# MAGIC Upon completing this lab you should understand how to read from and write to files in Spark, convert between `RDDs` and `DataFrames`, and build a model using both the ML and MLlib APIs.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading the data
# MAGIC  
# MAGIC First, we need to load data into Spark.  We'll use a built-in utility to load a [libSVM file](http://www.csie.ntu.edu.tw/~cjlin/libsvm/faq.html), which is stored in an S3 bucket on AWS.  We'll use `MLUtils.loadLibSVMFile` to load our file.  Here are the [Python](http://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.util.MLUtils.loadLibSVMFile) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.util.MLUtils$) APIs.

# COMMAND ----------

from pyspark.mllib.util import MLUtils

baseDir = '/mnt/ml-class/'
irisPath = baseDir + 'iris.scale'
irisRDD = MLUtils.loadLibSVMFile(sc, irisPath, minPartitions=20).cache()

# We get back an RDD of LabeledPoints.  Note that the libSVM format uses SparseVectors.
irisRDD.take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC What if we wanted to see the first few lines of the libSVM file to see what the format looks like?

# COMMAND ----------

sc.textFile(irisPath).take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC How is this data stored across partitions?

# COMMAND ----------

print 'number of partitions: {0}'.format(irisRDD.getNumPartitions())
elementsPerPart = (irisRDD
                   .mapPartitionsWithIndex(lambda i,x: [(i, len(list(x)))])
                   .collect())
print 'elements per partition: {0}\n'.format(elementsPerPart)
irisRDD.glom().take(1)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's convert this `RDD` of `LabeledPoints` to a `DataFrame`

# COMMAND ----------

irisDF = irisRDD.toDF()
irisDF.take(5)

# COMMAND ----------

irisDF.take(5)

# COMMAND ----------

irisDF.show(n=20, truncate=False)

# COMMAND ----------

display(irisDF)

# COMMAND ----------

print irisDF.schema, '\n'
irisDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Why were we able to convert directly from a `LabeledPoint` to a `Row`?

# COMMAND ----------

class Person(object):
    def __init__(self, name, age):
        self.name = name
        self.age = age

personDF = sqlContext.createDataFrame([Person('Bob', 28), Person('Julie', 35)])
display(personDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Python function calls for converting a RDD into a DataFrame
# MAGIC  
# MAGIC [createDataFrame](https://github.com/apache/spark/blob/3a11e50e21ececbec9708eb487b08196f195cd87/python/pyspark/sql/context.py#L342)
# MAGIC  
# MAGIC --> [_createFromRDD](https://github.com/apache/spark/blob/3a11e50e21ececbec9708eb487b08196f195cd87/python/pyspark/sql/context.py#L280)
# MAGIC  
# MAGIC ----> [_inferSchema](https://github.com/apache/spark/blob/3a11e50e21ececbec9708eb487b08196f195cd87/python/pyspark/sql/context.py#L221)
# MAGIC  
# MAGIC ------> [_infer_schema](https://github.com/apache/spark/blob/3a11e50e21ececbec9708eb487b08196f195cd87/python/pyspark/sql/types.py#L813)
# MAGIC  
# MAGIC --> [back to _createFromRDD](https://github.com/apache/spark/blob/3a11e50e21ececbec9708eb487b08196f195cd87/python/pyspark/sql/context.py#L304)
# MAGIC  
# MAGIC ----> [toInternal](https://github.com/apache/spark/blob/3a11e50e21ececbec9708eb487b08196f195cd87/python/pyspark/sql/types.py#L533)
# MAGIC  
# MAGIC [back to createDataFrame](https://github.com/apache/spark/blob/3a11e50e21ececbec9708eb487b08196f195cd87/python/pyspark/sql/context.py#L404)

# COMMAND ----------

# Our object does have a __dict__ attribute
print Person('Bob', 28).__dict__

# COMMAND ----------

personDF = sqlContext.createDataFrame([Person('Bob', 28), Person('Julie', 35)])
display(personDF)

# COMMAND ----------

# Show the schema that was inferred
print personDF.schema
personDF.printSchema()

# COMMAND ----------

from collections import namedtuple
PersonTuple = namedtuple('Person', ['name', 'age'])
personTupleDF = sqlContext.createDataFrame([PersonTuple('Bob', 28), PersonTuple('Julie', 35)])
display(personTupleDF)


# COMMAND ----------

personTupleDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transform the data
# MAGIC  
# MAGIC If you look at the data you'll notice that there are three values for label: 1, 2, and 3.  Spark's machine learning algorithms expect a 0 indexed target variable, so we'll want to adjust those labels.  This transformation is a simple expression where we'll subtract one from our `label` column.
# MAGIC  
# MAGIC For help, reference the SQL Programming Guide portion on [dataframe-operations](http://spark.apache.org/docs/latest/sql-programming-guide.html#dataframe-operations) or the Spark SQL [Python](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.package) APIs.  `select`, `col`, and `alias` can be used to accomplish this.
# MAGIC  
# MAGIC The resulting `DataFrame` should have two columns: one named `features` and another named `label`.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col

irisDFZeroIndex = irisDF.select('features', (col('label') - 1).alias('label'))
display(irisDFZeroIndex)

# COMMAND ----------

# TEST
from test_helper import Test
Test.assertEquals(irisDFZeroIndex.select('label').map(lambda r: r[0]).take(3), [0, 0, 0],
                  'incorrect value for irisDFZeroIndex')

# COMMAND ----------

# MAGIC %md
# MAGIC You'll also notice that we have four values for features and that those values are stored as a `SparseVector`.  We'll reduce those down to two values (for visualization purposes) and convert them to a `DenseVector`.  To do that we'll need to create a `udf` and apply it to our dataset.  Here's a `udf` reference for [Python](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.udf) and for [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.UserDefinedFunction).
# MAGIC  
# MAGIC Note that you can call the `toArray` method on a `SparseVector` to obtain an array, and you can convert an array into a `DenseVector` using the `Vectors.dense` method.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import udf
# Note that VectorUDT and MatrixUDT are found in linalg while other types are in sql.types
# VectorUDT should be the return type of the udf
from pyspark.mllib.linalg import Vectors, VectorUDT

# Take the first two values from a SparseVector and convert them to a DenseVector
firstTwoFeatures = udf(lambda sv: Vectors.dense(sv.toArray()[:2]), VectorUDT())

irisTwoFeatures = irisDFZeroIndex.select(firstTwoFeatures('features').alias('features'), 'label').cache()
display(irisTwoFeatures)

# COMMAND ----------

# TEST
Test.assertEquals(str(irisTwoFeatures.first()), 'Row(features=DenseVector([-0.5556, 0.25]), label=0.0)',
                  'incorrect definition of firstTwoFeatures')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2

# COMMAND ----------

repr(irisTwoFeatures.first()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC Let's view our `irisTwoFeatures` `DataFrame`.

# COMMAND ----------

irisTwoFeatures.take(5)

# COMMAND ----------

display(irisTwoFeatures)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Saving our DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC We'll be using parquet files to save our data.  More information about the parquet file format can be found on [parquet.apache.org](https://parquet.apache.org/documentation/latest/).

# COMMAND ----------

help(irisTwoFeatures.write.parquet)

# COMMAND ----------

import uuid
if 'parqUUID' not in locals():
    parqUUID = uuid.uuid1()
irisTwoFeatures.write.mode('overwrite').parquet('/tmp/{0}/irisTwoFeatures.parquet'.format(parqUUID))

# COMMAND ----------

# MAGIC %md
# MAGIC Note that we'll get a part file for each partition and that these files are compressed.

# COMMAND ----------

#display(dbutils.fs.ls(baseDir + 'irisTwoFeatures.parquet'))
display(dbutils.fs.ls('/tmp/{0}/irisTwoFeatures.parquet'.format(parqUUID)))

# COMMAND ----------

irisDFZeroIndex.write.mode('overwrite').parquet('/tmp/{0}/irisFourFeatures.parquet'.format(parqUUID))

# COMMAND ----------

# MAGIC %md
# MAGIC #### K-Means

# COMMAND ----------

# MAGIC %md
# MAGIC Now we'll build a k-means model using our two features and inspect the class hierarchy.
# MAGIC  
# MAGIC We'll build the k-means model using `KMeans`, an `ml` `Estimator`.  Details can be found in the [Python](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.clustering) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.clustering.package) APIs.  Also, examples that use [PCA](http://spark.apache.org/docs/latest/ml-features.html#pca) and  [logistic regression](http://spark.apache.org/docs/latest/ml-guide.html#example-estimator-transformer-and-param) can be found in the ML Programming Guide.
# MAGIC  
# MAGIC Make sure to work with the `irisTwoFeatures` `DataFrame`.

# COMMAND ----------

# ANSWER
from pyspark.ml.clustering import KMeans

# Create a KMeans Estimator and set k=3, seed=5, maxIter=20, initSteps=1
kmeans = (KMeans()
          .setK(3)
          .setSeed(5)
          .setMaxIter(20)
          .setInitSteps(1))

#  Call fit on the estimator and pass in our DataFrame
model = kmeans.fit(irisTwoFeatures)

# Obtain the clusterCenters from the KMeansModel
centers = model.clusterCenters()

# Use the model to transform the DataFrame by adding cluster predictions
transformed = model.transform(irisTwoFeatures)

print centers

# COMMAND ----------

# TEST
import numpy as np
Test.assertTrue(np.allclose([ 0.35115296, -0.10691828], centers[0]),
                'incorrect centers.  check your params.')
Test.assertEquals(transformed.select('prediction').map(lambda r: r[0]).take(4), [1,1,1,1],
                  'incorrect predictions')

# COMMAND ----------

# MAGIC %md
# MAGIC ## PART 3

# COMMAND ----------

# MAGIC %md
# MAGIC From the class hierarchy it is clear that `KMeans` is an `Estimator` while `KMeansModel` is a `Transformer`.

# COMMAND ----------

print '*** KMeans instance inheritance partial tree ***'
print '\n{0}\n'.format(type(kmeans))
print '\n'.join(map(str, type(kmeans).__bases__))
print '\n{0}'.format(type(kmeans).__bases__[0].__bases__)

print '\n\n*** KMeansModel instance inheritance partial tree ***'
print '\n{0}\n'.format(type(model))
print '\n'.join(map(str, type(model).__bases__)) + '\n'
print '\n'.join(map(str, type(model).__bases__[0].__bases__))
print '\n{0}'.format(type(model).__bases__[0].__bases__[0].__bases__)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's print the three centroids of our model

# COMMAND ----------

print centers

# COMMAND ----------

# MAGIC %md
# MAGIC Note that our predicted cluster is appended, as a column, to our input `DataFrame`.  Here it would be desirable to see consistency between label and prediction.  These don't need to be the same number but if label 0 is usually predicted to be cluster 1 that would indicate that our unsupervised learning is naturally grouping the data into species.

# COMMAND ----------

display(transformed)

# COMMAND ----------

# MAGIC %md
# MAGIC #### K-Means Visualized

# COMMAND ----------

modelCenters = []
iterations = [0, 2, 4, 7, 10, 20]
for i in iterations:
    kmeans = KMeans(k=3, seed=5, maxIter=i, initSteps=1)
    model = kmeans.fit(irisTwoFeatures)
    modelCenters.append(model.clusterCenters())

# COMMAND ----------

print 'modelCenters:'
for centroids in modelCenters:
  print centroids

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np

def prepareSubplot(xticks, yticks, figsize=(10.5, 6), hideLabels=False, gridColor='#999999',
                gridWidth=1.0, subplots=(1, 1)):
    """Template for generating the plot layout."""
    plt.close()
    fig, axList = plt.subplots(subplots[0], subplots[1], figsize=figsize, facecolor='white',
                               edgecolor='white')
    if not isinstance(axList, np.ndarray):
        axList = np.array([axList])

    for ax in axList.flatten():
        ax.axes.tick_params(labelcolor='#999999', labelsize='10')
        for axis, ticks in [(ax.get_xaxis(), xticks), (ax.get_yaxis(), yticks)]:
            axis.set_ticks_position('none')
            axis.set_ticks(ticks)
            axis.label.set_color('#999999')
            if hideLabels: axis.set_ticklabels([])
        ax.grid(color=gridColor, linewidth=gridWidth, linestyle='-')
        map(lambda position: ax.spines[position].set_visible(False), ['bottom', 'top', 'left', 'right'])

    if axList.size == 1:
        axList = axList[0]  # Just return a single axes object for a regular plot
    return fig, axList

# COMMAND ----------

# MAGIC %md
# MAGIC Display the clustroid centers with the original labeled points and circles representing distance from the centroids.

# COMMAND ----------

data = irisTwoFeatures.collect()
features, labels = zip(*data)
x, y = zip(*features)

centroidX, centroidY = zip(*centers)
colorMap = 'Set1'  # was 'Set2', 'Set1', 'Dark2', 'winter'

fig, ax = prepareSubplot(np.arange(-1, 1.1, .4), np.arange(-1, 1.1, .4), figsize=(8,6))
plt.scatter(x, y, s=14**2, c=labels, edgecolors='#8cbfd0', alpha=0.80, cmap=colorMap)
plt.scatter(centroidX, centroidY, s=22**2, marker='*', c='yellow')
cmap = cm.get_cmap(colorMap)

colorIndex = [.99, 0., .5]
for i, (x,y) in enumerate(centers):
    print cmap(colorIndex[i])
    for size in [.10, .20, .30, .40, .50]:
        circle1=plt.Circle((x,y),size,color=cmap(colorIndex[i]), alpha=.10, linewidth=2)
        ax.add_artist(circle1)

ax.set_xlabel('Sepal Length'), ax.set_ylabel('Sepal Width')
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC Visualize how the clustroid centers move as the k-means algorithm iterates.

# COMMAND ----------

x, y = zip(*features)

oldCentroidX, oldCentroidY = None, None

fig, axList = prepareSubplot(np.arange(-1, 1.1, .4), np.arange(-1, 1.1, .4), figsize=(11, 15),
                             subplots=(3, 2))
axList = axList.flatten()

for i,ax in enumerate(axList[:]):
    ax.set_title('K-means for {0} iterations'.format(iterations[i]), color='#999999')
    centroids = modelCenters[i]
    centroidX, centroidY = zip(*centroids)

    ax.scatter(x, y, s=10**2, c=labels, edgecolors='#8cbfd0', alpha=0.80, cmap=colorMap)
    ax.scatter(centroidX, centroidY, s=16**2, marker='*', c='yellow')
    if oldCentroidX and oldCentroidY:
      ax.scatter(oldCentroidX, oldCentroidY, s=16**2, marker='*', c='grey')
    cmap = cm.get_cmap(colorMap)

    colorIndex = [.99, 0., .5]
    for i, (x1,y1) in enumerate(centroids):
      print cmap(colorIndex[i])
      circle1=plt.Circle((x1,y1),.35,color=cmap(colorIndex[i]), alpha=.40)
      ax.add_artist(circle1)

    ax.set_xlabel('Sepal Length'), ax.set_ylabel('Sepal Width')
    oldCentroidX, oldCentroidY = centroidX, centroidY
#axList[-1].cla()
#axList[-1].get_yaxis().set_ticklabels([])
#axList[-1].get_xaxis().set_ticklabels([])

plt.tight_layout()

display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC Summary plot of the centroid movement.

# COMMAND ----------

centroidX, centroidY = zip(*centers)

# generate layout and plot data
def plotKMeansTrack(x=x, y=y, labels=labels):
    fig, ax = prepareSubplot(np.arange(-1, 1.1, .4), np.arange(-1, 1.1, .4), figsize=(8, 6))
    ax.set_ylim(-1, 1), ax.set_xlim(-1, 1)
    #plt.scatter(x, y, s=14**2, c=labels, edgecolors='#8cbfd0', alpha=0.80, cmap='winter')
    cmap = cm.get_cmap(colorMap)

    colorIndex = [.99, 0.0, .5]

    alphas = [.05, .10, .15, .20, .30, .40]
    sizes = [8, 12, 16, 20, 24, 28]

    for iteration, centroids in enumerate(modelCenters):
        centroidX, centroidY = zip(*centroids)
        color = 'lightgrey' if iteration < 5 else 'yellow'
        plt.scatter(centroidX, centroidY, s=sizes[iteration]**2, marker='*', c=color)

        for i, (x,y) in enumerate(centroids):
            print cmap(colorIndex[i])
            circle1=plt.Circle((x,y),.35,color=cmap(colorIndex[i%3]), alpha=alphas[iteration], linewidth=2.0)
            ax.add_artist(circle1)


    ax.set_xlabel('Sepal Length'), ax.set_ylabel('Sepal Width')
    display(fig)

plotKMeansTrack(centroidX, centroidY)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using MLlib instead of ML

# COMMAND ----------

# MAGIC %md
# MAGIC First, convert our `DataFrame` into an `RDD`.

# COMMAND ----------

# Note that .rdd is not necessary, but is here to illustrate that we are working with an RDD
irisTwoFeaturesRDD = (irisTwoFeatures
                      .rdd
                      .map(lambda r: (r[1], r[0])))
irisTwoFeaturesRDD.take(2)

# COMMAND ----------

# MAGIC %md
# MAGIC Then import MLlib's `KMeans` as `MLlibKMeans` to differentiate it from `ml.KMeans`

# COMMAND ----------

from pyspark.mllib.clustering import KMeans as MLlibKMeans

help(MLlibKMeans)

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, let's build our k-means model.  Here are the relevant [Python](http://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.clustering.KMeans) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.clustering.KMeans) APIs.
# MAGIC  
# MAGIC Make sure to set `k` to 3, `maxIterations` to 20, `seed` to 5, and `initializationSteps` to 1.  Also, note that we returned an `RDD` with (label, feature) tuples.  You'll just need the features, which you can obtain by calling `.values()` on `irisTwoFeaturesRDD`.

# COMMAND ----------

# ANSWER
mllibKMeans = MLlibKMeans.train(irisTwoFeaturesRDD.values(), 3, maxIterations=20, seed=5,
                                initializationSteps=1)

print 'mllib: {0}'.format(mllibKMeans.clusterCenters)
print 'ml:    {0}'.format(centers)

# COMMAND ----------

# TEST
import numpy as np
Test.assertTrue(np.allclose(mllibKMeans.clusterCenters, centers), "Your mllib and ml models don't match")

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have an `mllibKMeans` model how do we generate predictions and compare those to our labels?

# COMMAND ----------

predictionsRDD = mllibKMeans.predict(irisTwoFeaturesRDD.values())
print predictionsRDD.take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC We'll use `zip` to combine the feature and prediction RDDs together.  Note that zip assumes that the RDDs have the same number of partitions and that each partition has the same number of elements.  This is true here as our predictions were the result of a `map` operation on the feature RDD.

# COMMAND ----------

combinedRDD = irisTwoFeaturesRDD.zip(predictionsRDD)
combinedRDD.take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's compare this to the result from `ml`.

# COMMAND ----------

display(transformed)

# COMMAND ----------

# MAGIC %md
# MAGIC #### How do the `ml` and `mllib` implementations differ?

# COMMAND ----------

import inspect
print inspect.getsource(kmeans.fit)

# COMMAND ----------

print inspect.getsource(kmeans._fit)

# COMMAND ----------

print inspect.getsource(kmeans._fit_java)

# COMMAND ----------

# MAGIC %md
# MAGIC The `ml` version of k-means is just a wrapper to MLlib's implementation.  Let's take a look here:
# MAGIC [org.apache.spark.ml.clustering.KMeans source](https://github.com/apache/spark/blob/e1e77b22b3b577909a12c3aa898eb53be02267fd/mllib/src/main/scala/org/apache/spark/ml/clustering/KMeans.scala#L192).
# MAGIC  
# MAGIC How is $ being used in this function? `Param` [source code](https://github.com/apache/spark/blob/2b574f52d7bf51b1fe2a73086a3735b633e9083f/mllib/src/main/scala/org/apache/spark/ml/param/params.scala#L643) has the answer.
# MAGIC  
# MAGIC Which is different than $'s usage for SQL columns where it is a [string interpolator that returns a ColumnName](https://github.com/apache/spark/blob/3d683a139b333456a6bd8801ac5f113d1ac3fd18/sql/core/src/main/scala/org/apache/spark/sql/SQLContext.scala#L386)
