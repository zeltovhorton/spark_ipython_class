# Databricks notebook source exported at Tue, 16 Feb 2016 19:19:21 UTC
# MAGIC %md
# MAGIC # External modeling libraries
# MAGIC ---
# MAGIC How can we leverage our existing experience with modeling libraries like [scikit-learn](http://scikit-learn.org/stable/index.html)?  We'll explore three approaches that make use of existing libraries, but still benefit from the parallelism provided by Spark.
# MAGIC 
# MAGIC These approaches are:
# MAGIC  * Grid Search
# MAGIC  * Cross Validation
# MAGIC  * Sampling
# MAGIC  
# MAGIC We'll start by using scikit-learn on the driver and then we'll demonstrate the parallel techniques.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Use scikit-learn locally

# COMMAND ----------

# MAGIC %md
# MAGIC Load the data from `sklearn.datasets`, and create test and train sets.

# COMMAND ----------

import numpy as np
from sklearn import datasets

# COMMAND ----------

# Load the data
iris = datasets.load_iris()

# Generate test and train sets
size = len(iris.target)
indices = np.random.permutation(size)

cutoff = int(size * .30)

testX = iris.data[indices[0:cutoff],:]
trainX = iris.data[indices[cutoff:],:]
testY = iris.target[indices[0:cutoff]]
trainY = iris.target[indices[cutoff:]]

# COMMAND ----------

# MAGIC %md
# MAGIC Build a nearest neighbors classifier using [sklearn.neighbors.KNeighborsClassifier](http://scikit-learn.org/stable/modules/generated/sklearn.neighbors.KNeighborsClassifier.html).

# COMMAND ----------

from sklearn.neighbors import KNeighborsClassifier

# Create a KNeighborsClassifier using the default settings
knn = KNeighborsClassifier()
knn.fit(trainX, trainY)

predictions = knn.predict(testX)

# Print out the accuracy of the classifier on the test set
print sum(predictions == testY) / float(len(testY))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Grid Search

# COMMAND ----------

# MAGIC %md
# MAGIC Define a function `runNearestNeighbors` that takes in a parameter `k` and returns a tuple of (`k`, accuracy).  Note that we'll load the data from `sklearn.datasets`, and we'll create train and test splits using [sklearn.cross_validation.train_test_split](http://scikit-learn.org/stable/modules/generated/sklearn.cross_validation.train_test_split.html).

# COMMAND ----------

from sklearn.cross_validation import train_test_split

def runNearestNeighbors(k):
    # Load dataset from sklearn.datasets
    irisData = datasets.load_iris()
    
    # Split into train and test using sklearn.cross_validation.train_test_split
    yTrain, yTest, XTrain, XTest = train_test_split(irisData.target, 
                                                    irisData.data)
    
    # Build the model
    knn = KNeighborsClassifier(n_neighbors=k)
    knn.fit(XTrain, yTrain)
    
    # Calculate predictions and accuracy
    predictions = knn.predict(XTest)
    accuracy = (predictions == yTest).sum() / float(len(yTest))
    
    return (k, accuracy)   

# COMMAND ----------

# MAGIC %md
# MAGIC Now we'll run a grid search for `k` from 1 to 10.

# COMMAND ----------

k = sc.parallelize(xrange(1, 11))
results = k.map(runNearestNeighbors)
print '\n'.join(map(str, results.collect()))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's transfer the data using a Broadcast instead of loading it at each executor.  You can create a [Broadcast](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.Broadcast) variable using [sc.broadcast()](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.SparkContext.broadcast).

# COMMAND ----------

# Create the Broadcast variable
irisBroadcast = sc.broadcast(iris)

def runNearestNeighborsBroadcast(k):
    # Using the data in the irisBroadcast variable split into train and test using
    # sklearn.cross_validation.train_test_split
    yTrain, yTest, XTrain, XTest = train_test_split(irisBroadcast.value.target,
                                                    irisBroadcast.value.data)
    
    # Build the model
    knn = KNeighborsClassifier(n_neighbors=k)
    knn.fit(XTrain, yTrain)
    
    # Calculate predictions and accuracy
    predictions = knn.predict(XTest)
    accuracy = (predictions == yTest).sum() / float(len(yTest))
    
    return (k, accuracy)   
  
# Rerun grid search
k = sc.parallelize(xrange(1, 11))
results = k.map(runNearestNeighborsBroadcast)
print '\n'.join(map(str, results.collect()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part 3: Cross Validation

# COMMAND ----------

# MAGIC %md
# MAGIC Now we'll use [sklearn.cross_validation.KFold](http://scikit-learn.org/stable/modules/generated/sklearn.cross_validation.KFold.html) to evaluate our model using 10-fold cross validation.  First, generate the 10 folds using `KFold`.

# COMMAND ----------

from sklearn.cross_validation import KFold

# Create indicies for 10-fold cross validation
kf = KFold(size, n_folds=10)

folds = sc.parallelize(kf)
print folds.take(1)

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np

def preparePlot(xticks, yticks, figsize=(10.5, 6), hideLabels=False, gridColor='#999999',
                gridWidth=1.0):
    """Template for generating the plot layout."""
    plt.close()
    fig, ax = plt.subplots(figsize=figsize, facecolor='white', edgecolor='white')
    ax.axes.tick_params(labelcolor='#999999', labelsize='10')
    for axis, ticks in [(ax.get_xaxis(), xticks), (ax.get_yaxis(), yticks)]:
        axis.set_ticks_position('none')
        axis.set_ticks(ticks)
        axis.label.set_color('#999999')
        if hideLabels: axis.set_ticklabels([])
    plt.grid(color=gridColor, linewidth=gridWidth, linestyle='-')
    map(lambda position: ax.spines[position].set_visible(False), ['bottom', 'top', 'left', 'right'])
    return fig, ax


anArray = np.zeros(150)

data = []
for fold in folds.collect():
  bIdx, rIdx = fold
  anArray[bIdx] = 0
  anArray[rIdx] = 1
  data.append(anArray.copy())

dataValues = np.vstack(data)

# generate layout and plot
fig, ax = preparePlot(np.arange(-.5, 150, 15), np.arange(-.5, 10, 1), figsize=(8,7), hideLabels=True,
                      gridColor='#333333', gridWidth=1.1)
image = plt.imshow(dataValues,interpolation='nearest', aspect='auto', cmap=cm.winter)
display(fig) 

# COMMAND ----------

# MAGIC %md
# MAGIC Create a function that runs nearest neighbors based on the fold information passed in.  Note that we'll have the function return an [np.array](http://docs.scipy.org/doc/numpy/reference/generated/numpy.array.html) which provides us with additional functionality that we'll take advantage of in a couple steps.

# COMMAND ----------

import numpy as np

def runNearestNeighborsWithFolds((trainIndex, testIndex)):
    # Assign training and test sets from irisBroadcast using trainIndex and testIndex
    XTrain = irisBroadcast.value.data[trainIndex]
    yTrain = irisBroadcast.value.target[trainIndex]
    XTest = irisBroadcast.value.data[testIndex]
    yTest = irisBroadcast.value.target[testIndex]
    
   # Build the model
    knn = KNeighborsClassifier(n_neighbors=5)
    knn.fit(XTrain, yTrain)
    
    # Calculate predictions
    predictions = knn.predict(XTest)
    
    # Compute the number of correct predictions and total predictions
    correct = (predictions == yTest).sum() 
    total = len(testIndex)
    
    # Return an np.array of the number of correct predictions and total predictions
    return np.array([correct, total])

# COMMAND ----------

# MAGIC %md
# MAGIC Computer nearest neighbors using each fold.

# COMMAND ----------

# Run nearest neighbors on each of the folds
foldResults = folds.map(runNearestNeighborsWithFolds)
print 'correct / total\n' + '\n'.join(map(str, foldResults.collect()))

# COMMAND ----------

# MAGIC %md
# MAGIC Now aggregate the results from the folds to see overall accuracy

# COMMAND ----------

# Note that using .sum() on an RDD of numpy arrays sums by columns 
correct, total = foldResults.sum()
print correct / float(total)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part 4: Sampling

# COMMAND ----------

# MAGIC %md
# MAGIC We might have a dataset that is too large where we can't use our external modeling library on the full data set.  In this case we might want to build several models on samples of the dataset.  We could either build the same model, using different parameters, or try completely different techniques to see what works best.

# COMMAND ----------

# MAGIC %md
# MAGIC First we'll parallelize the iris dataset and distributed it across our cluster.  

# COMMAND ----------

# Split the iris dataset into 8 partitions
irisData = sc.parallelize(zip(iris.target, iris.data), 8)
print irisData.take(2), '\n'

# View the number of elements found in each of the eight partitions
print (irisData
       .mapPartitions(lambda x: [len(list(x))])
       .collect())

# View the target (y) stored by partition
print '\n', irisData.keys().glom().collect()

# COMMAND ----------

dataValues = np.vstack([np.array(x[:18]) for x in irisData.keys().glom().collect()])

# generate layout and plot
fig, ax = preparePlot(np.arange(-.5, 18, 2), np.arange(-.5, 10, 1), figsize=(8,7), hideLabels=True,
                      gridColor='#555555', gridWidth=1.1)
image = plt.imshow(dataValues,interpolation='nearest', aspect='auto', cmap=cm.Blues)
display(fig) 

# COMMAND ----------

# MAGIC %md
# MAGIC Since each of the partitions represents a dataset that we'll be using to run our local model, we have a problem.  The data is ordered, so our partitions are mostly homogenous with regard to our target variable.
# MAGIC 
# MAGIC We'll repartition the data using `partitionBy` so that the data is randomly ordered across partitions.

# COMMAND ----------

# Randomly reorder the data across partitions
randomOrderData = (irisData
                   .map(lambda x: (np.random.randint(5), x))
                   .partitionBy(5)
                   .values())

# Show the new groupings of target variables
print randomOrderData.keys().glom().collect()

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, we'll build a function that takes in the target and data from the `randomOrderData` RDD and returns the number of correct and total predictions (with regard to a test set).

# COMMAND ----------

print randomOrderData.keys().mapPartitions(lambda x: [len(list(x))]).collect()

# COMMAND ----------

data = []
for x in randomOrderData.keys().glom().collect():
  temp = np.repeat(-1, 35)
  pData = x[:35]
  temp[:len(pData)] = pData
  data.append(temp.copy())

dataValues = np.vstack(data)

fig, ax = preparePlot(np.arange(-.5, 35, 4), np.arange(-.5, 5, 1), figsize=(8,7), hideLabels=True,
                      gridColor='#555555', gridWidth=1.1)
image = plt.imshow(dataValues,interpolation='nearest', aspect='auto', cmap=cm.Blues)
display(fig) 

# COMMAND ----------

# Recall what randomOrderData contains
print randomOrderData.take(3)

# COMMAND ----------

def runNearestNeighborsPartition(labelAndFeatures):
    y, X = zip(*labelAndFeatures)
    yTrain, yTest, XTrain, XTest = train_test_split(y, X)
    
    knn = KNeighborsClassifier()
    knn.fit(XTrain, yTrain)
    
    predictions = knn.predict(XTest)
    correct = (predictions == yTest).sum() 
    total = len(yTest)
    return [np.array([correct, total])]

sampleResults = randomOrderData.mapPartitions(runNearestNeighborsPartition)

print 'correct / total\n' + '\n'.join(map(str, sampleResults.collect()))

# COMMAND ----------


