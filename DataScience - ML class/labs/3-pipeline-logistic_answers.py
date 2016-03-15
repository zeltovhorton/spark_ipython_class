# Databricks notebook source exported at Tue, 16 Feb 2016 19:44:30 UTC
# MAGIC %md
# MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC # Pipelines and Logistic Regression
# MAGIC  
# MAGIC In this lab we'll cover transformers, estimators, evaluators, and pipelines.  We'll use transformers and estimators to prepare our data for use in a logistic regression model and will use pipelines to combine these steps together.  Finally, we'll evaluate our model.
# MAGIC  
# MAGIC This lab also covers creating train and test datasets using `randomSplit`, visualizing a ROC curve, and generating both `ml` and `mllib` logistic regression models.
# MAGIC  
# MAGIC After completing this lab you should be comfortable using transformers, estimators, evaluators, and pipelines.

# COMMAND ----------

baseDir = "/mnt/ml-class/"
irisTwoFeatures = sqlContext.read.parquet(baseDir + 'irisTwoFeatures.parquet').cache()
print '\n'.join(map(repr, irisTwoFeatures.take(2)))

# COMMAND ----------

display(irisTwoFeatures)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Prepare the data
# MAGIC  
# MAGIC To explore our data in more detail, we're going to we pull out sepal length and sepal width and create two columns.  These are the two features found in our `DenseVector`.
# MAGIC  
# MAGIC In order to do this you will write a `udf` that takes in two values.  The first will be the name of the vector that we are operating on and the second is a literal for the index position.  Here are links to `lit` in the [Python](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.lit) and [Scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$) APIs.
# MAGIC  
# MAGIC The `udf` will return a `DoubleType` that is the value of the specified vector at the specified index position.
# MAGIC  
# MAGIC In order to call our function, we need to wrap the second value in `lit()` (e.g. `lit(1)` for the second element).  This is because our `udf` expects a `Column` and `lit` generates a `Column` where the literal is the value.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import DoubleType

# Remember to cast the value you extract from the Vector using float()
getElement = udf(lambda v, i: float(v[i]), DoubleType())

irisSeparateFeatures = (irisTwoFeatures
                        .withColumn('sepalLength', getElement('features', lit(0)))
                        .withColumn('sepalWidth', getElement('features', lit(1))))
display(irisSeparateFeatures)


# COMMAND ----------

# TEST
from test_helper import Test
firstRow = irisSeparateFeatures.select('sepalWidth', 'features').map(lambda r: (r[0], r[1])).first()
Test.assertEquals(firstRow[0], firstRow[1][1], 'incorrect definition for getElement')

# COMMAND ----------

# MAGIC %md
# MAGIC What about using `Column`'s `getItem` method?

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException

try:
    display(irisTwoFeatures.withColumn('sepalLength', col('features').getItem(0)))
except AnalysisException as e:
    print e

# COMMAND ----------

# MAGIC %md
# MAGIC Unfortunately, it doesn't work for vectors, but it does work on arrays.

# COMMAND ----------

from pyspark.sql import Row
arrayDF = sqlContext.createDataFrame([Row(anArray=[1,2,3]), Row(anArray=[4,5,6])])
arrayDF.show()

arrayDF.select(col('anArray').getItem(0)).show()
arrayDF.select(col('anArray')[1]).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2

# COMMAND ----------

# MAGIC %md
# MAGIC Next, let's register our function and then call it directly from SQL.

# COMMAND ----------

sqlContext.udf.register('getElement', getElement.func, getElement.returnType)
irisTwoFeatures.registerTempTable('irisTwo')

# COMMAND ----------

# MAGIC %sql
# MAGIC select getElement(features, 0) as sepalLength from irisTwo

# COMMAND ----------

# MAGIC %md
# MAGIC #### EDA and feature engineering

# COMMAND ----------

# MAGIC %md
# MAGIC Let's see the ranges of our values and view their means and standard deviations.

# COMMAND ----------

display(irisSeparateFeatures.describe('label', 'sepalLength', 'sepalWidth'))

# COMMAND ----------

# MAGIC %md
# MAGIC Our features both take on values from -1.0 to 1.0, but have different means and standard deviations.  How could we standardize our data to have zero mean and unit standard deviations?  For this task we'll use the `ml` estimator `StandardScaler`.  Feature transformers (which are sometimes estimators) can be found in [pyspark.ml.feature](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.feature) for Python or [org.apache.spark.ml.feature](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.feature.package) for Scala.
# MAGIC  
# MAGIC Also, remember that the [ML Guide](http://spark.apache.org/docs/latest/ml-features.html#standardscaler) is a good place to find additional information.

# COMMAND ----------

from pyspark.ml.feature import StandardScaler
help(StandardScaler)

# COMMAND ----------

standardScaler = (StandardScaler()
                  .setInputCol('features')
                  .setOutputCol('standardized')
                  .setWithMean(True))

print standardScaler.explainParams()

# COMMAND ----------

irisStandardizedLength = (standardScaler
                          .fit(irisSeparateFeatures)
                          .transform(irisSeparateFeatures)
                          .withColumn('standardizedLength', getElement('standardized', lit(0))))
display(irisStandardizedLength)

# COMMAND ----------

display(irisStandardizedLength.describe('sepalLength', 'standardizedLength'))

# COMMAND ----------

# MAGIC %md
# MAGIC What if instead we wanted to normalize the data?  For example, we might want to normalize each set of features (per row) to have length one using an \\( l^2 \\) norm.  That would cause the sum of the features squared to be one: \\( \sum_{i=1}^d x_i^2 = 1 \\).  This is could be useful if we wanted to compare observations based on a distance metric like in k-means clustering.
# MAGIC  
# MAGIC `Normalizer` can be found in [pyspark.ml.feature](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.Normalizer) for Python and the [org.apache.spark.ml.feature](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.Normalizer) package for Scala.
# MAGIC  
# MAGIC Let's implement `Normalizer` and transform our features.  Make sure to use a `P` of 2.0 and to name the output column "featureNorm".  Remember that we're working with the `irisTwoFeatures` dataset.

# COMMAND ----------

# ANSWER
from pyspark.ml.feature import Normalizer
normalizer = (Normalizer()
              .setInputCol('features')
              .setOutputCol('featureNorm')
              .setP(2.0))

irisNormalized = normalizer.transform(irisTwoFeatures)  # Note that we're calling transform here
display(irisNormalized)

# COMMAND ----------

# TEST
import numpy as np
firstVector = irisNormalized.select('featureNorm').map(lambda r: r[0]).first()
Test.assertTrue(np.allclose(firstVector.norm(2.0), 1.0), 'incorrect setup of normalizer')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3

# COMMAND ----------

# MAGIC %md
# MAGIC Let's just check and see that our norms are equal to 1.0

# COMMAND ----------

l2Norm = udf(lambda v: float(v.norm(2.0)), DoubleType())

featureLengths = irisNormalized.select(l2Norm('features').alias('featuresLength'),
                                       l2Norm('featureNorm').alias('featureNormLength'))
display(featureLengths)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, let's bucketize our features.  This will allow us to convert continuous features into discrete buckets.  This is often desirable for logistic regression which we'll be performing later in this lab.
# MAGIC  
# MAGIC We'll use the following splits: -infinity, -0.5, 0.0, 0.5, +infinity.  Note that in Python infinity can be represented using `float('inf')` and that in Scala `Double.NegativeInfinity` and `Double.PositiveInfinity` can be used.

# COMMAND ----------

from pyspark.ml.feature import Bucketizer

splits = [-float('inf'), -.5, 0.0, .5, float('inf')]

lengthBucketizer = (Bucketizer()
              .setInputCol('sepalLength')
              .setOutputCol('lengthFeatures')
              .setSplits(splits))

irisBucketizedLength = lengthBucketizer.transform(irisSeparateFeatures)
display(irisBucketizedLength)

# COMMAND ----------

widthBucketizer = (Bucketizer()
                   .setInputCol("sepalWidth")
                   .setOutputCol("widthFeatures")
                   .setSplits(splits))

irisBucketizedWidth = widthBucketizer.transform(irisBucketizedLength)
display(irisBucketizedWidth)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's combine the two bucketizers into a [Pipeline](http://spark.apache.org/docs/latest/ml-guide.html#pipeline-components) that performs both bucketizations.  A `Pipeline` is made up of stages which can be set using `setStages` and passing in a `list` of stages in Python or an `Array` of stages in Scala.  `Pipeline` is an estimator, which means it implements a `fit` method which returns a `PipelineModel`.  A `PipelineModel` is a transformer, which means that it implements a `transform` method which can be used to run the stages.

# COMMAND ----------

from pyspark.ml.pipeline import Pipeline

pipelineBucketizer = Pipeline().setStages([lengthBucketizer, widthBucketizer])

pipelineModelBucketizer = pipelineBucketizer.fit(irisSeparateFeatures)
irisBucketized = pipelineModelBucketizer.transform(irisSeparateFeatures)

display(irisBucketized)


# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have created two new features through bucketing, let's combine those two features into a `Vector` with `VectorAssembler`.  VectorAssembler can be found in [pyspark.ml.feature](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.VectorAssembler) for Python and the [org.apache.spark.ml.feature](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.VectorAssembler) package for Scala.
# MAGIC  
# MAGIC Set the params of `assembler` so that both "lengthFeatures" and "widthFeatures" are assembled into a column called "featuresBucketized".
# MAGIC  
# MAGIC Then, set the stages of `pipeline` to include both bucketizers and the assembler as the last stage.
# MAGIC  
# MAGIC Finally, use `pipeline` to generate a new `DataFrame` called `irisAssembled`.

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
pipeline = Pipeline()
assembler = VectorAssembler()

print assembler.explainParams()
print '\n',pipeline.explainParams()

# COMMAND ----------

# ANSWER
# Set assembler params
(assembler
 .setInputCols(['lengthFeatures', 'widthFeatures'])
 .setOutputCol('featuresBucketized'))

pipeline.setStages([lengthBucketizer, widthBucketizer, assembler])
irisAssembled = pipeline.fit(irisSeparateFeatures).transform(irisSeparateFeatures)
display(irisAssembled)

# COMMAND ----------

# TEST
from pyspark.mllib.linalg import Vectors
firstAssembly = irisAssembled.select('lengthFeatures', 'widthFeatures', 'featuresBucketized').first()
Test.assertTrue(all(firstAssembly[2].toArray() == [firstAssembly[0], firstAssembly[1]]),
                'incorrect value for column featuresBucketized')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4

# COMMAND ----------

# MAGIC %md
# MAGIC  
# MAGIC #### Logistic Regression

# COMMAND ----------

# MAGIC %md
# MAGIC First let's look at our data by label.

# COMMAND ----------

display(irisSeparateFeatures.groupBy('label').count().orderBy('label'))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's build a model that tries to differentiate between the first two classes.

# COMMAND ----------

from pyspark.sql.functions import col
irisTwoClass = irisSeparateFeatures.filter(col('label') < 2)
display(irisTwoClass.groupBy('label').count().orderBy('label'))

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we'll split our dataset into test and train sets.

# COMMAND ----------

irisTest, irisTrain = irisTwoClass.randomSplit([.25, .75], seed=0)

# Cache as we'll be using these several times
irisTest.cache()
irisTrain.cache()

print 'Items in test datset: {0}'.format(irisTest.count())
print 'Items in train dataset: {0}'.format(irisTrain.count())

# COMMAND ----------

# MAGIC %md
# MAGIC And now let's build our logistic regression model.  LogisticRegression can be found in [pyspark.ml.classification](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.LogisticRegression) for Python and the [org.apache.spark.ml.classification](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.classification.LogisticRegression) package for Scala.  The ML Guide also has a nice overview of [logistic regression](http://spark.apache.org/docs/latest/ml-classification-regression.html#logistic-regression).
# MAGIC  
# MAGIC Make sure to set the featuresCol to "featuresBucketized", the regParam to 0.0, the labelCol to "label", and the maxIter to 1000.
# MAGIC  
# MAGIC Also, set the pipeline stages to include the two bucketizers, assembler, and logistic regression.

# COMMAND ----------

# ANSWER
from pyspark.ml.classification import LogisticRegression

lr = (LogisticRegression()
      .setFeaturesCol('featuresBucketized')
      .setRegParam(0.0)
      .setLabelCol('label')
      .setMaxIter(1000))

pipeline.setStages([lengthBucketizer, widthBucketizer, assembler, lr])

pipelineModelLR = pipeline.fit(irisTrain)

irisTestPredictions = (pipelineModelLR
                       .transform(irisTest)
                       .cache())
display(irisTestPredictions)

# COMMAND ----------

irisTestPredictions.select("probability").first()[0][0]

# COMMAND ----------

# TEST
Test.assertTrue(sum(irisTestPredictions.select("probability").first()[0]) > .99,
                'incorrect build of the lr model')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5

# COMMAND ----------

print pipelineModelLR.stages
print '\n{0}'.format(pipelineModelLR.stages[-1].weights)

# COMMAND ----------

# MAGIC %md
# MAGIC Leaving our features to range from 0 to 3 means that a value of 2 has twice the impact in our model than a value of 1.  Since these buckets were based on increasing numeric values this is not unreasonable; however, we might want to convert each of these values to a dummy feature that takes on either a 0 or 1 corresponding to whether the value occurs.  This allows the model to measure the impact of the occurrences of the individual values and allows for non-linear relationships.
# MAGIC  
# MAGIC To do this we'll use the `OneHotEncoder` estimator.

# COMMAND ----------

from pyspark.ml.feature import OneHotEncoder

oneHotLength = (OneHotEncoder()
                .setInputCol('lengthFeatures')
                .setOutputCol('lengthOneHot'))

pipeline.setStages([lengthBucketizer, widthBucketizer, oneHotLength])

irisWithOneHotLength = pipeline.fit(irisTrain).transform(irisTrain)
display(irisWithOneHotLength)

# COMMAND ----------

irisWithOneHotLength.select('lengthOneHot').first()

# COMMAND ----------

# MAGIC %md
# MAGIC Create a `OneHotEncoder` for width as well, and combine both encoders together into a `featuresBucketized` column.

# COMMAND ----------

oneHotWidth = (OneHotEncoder()
               .setInputCol('widthFeatures')
               .setOutputCol('widthOneHot'))

assembleOneHot = (VectorAssembler()
                  .setInputCols(['lengthOneHot', 'widthOneHot'])
                  .setOutputCol('featuresBucketized'))

pipeline.setStages([lengthBucketizer, widthBucketizer, oneHotLength, oneHotWidth, assembleOneHot])

display(pipeline.fit(irisTrain).transform(irisTrain))

# COMMAND ----------

# MAGIC %md
# MAGIC Create the full `Pipeline` through logistic regression and make predictions on the test data.

# COMMAND ----------

pipeline.setStages([lengthBucketizer, widthBucketizer, oneHotLength, oneHotWidth, assembleOneHot, lr])

pipelineModelLR2 = pipeline.fit(irisTrain)

irisTestPredictions2 = (pipelineModelLR2
                        .transform(irisTest)
                        .cache())
display(irisTestPredictions2)

# COMMAND ----------

# MAGIC %md
# MAGIC What does our new model look like?

# COMMAND ----------

logisticModel = pipelineModelLR2.stages[-1]
print logisticModel.intercept
print repr(logisticModel.weights)

# COMMAND ----------

# MAGIC %md
# MAGIC What about model accuracy?

# COMMAND ----------

from pyspark.sql.functions import col

def modelAccuracy(df):
  return (df
          .select((col('prediction') == col('label')).cast('int').alias('correct'))
          .groupBy()
          .avg('correct')
          .first()[0])

modelOneAccuracy = modelAccuracy(irisTestPredictions)
modelTwoAccuracy = modelAccuracy(irisTestPredictions2)

print 'modelOneAccuracy: {0:.3f}'.format(modelOneAccuracy)
print 'modelTwoAccuracy: {0:.3f}'.format(modelTwoAccuracy)

# COMMAND ----------

# MAGIC %md
# MAGIC Or we can use SQL instead.

# COMMAND ----------

irisTestPredictions.registerTempTable('modelOnePredictions')
sqlResult = sqlContext.sql('select avg(int(prediction == label)) from modelOnePredictions')
display(sqlResult)

# COMMAND ----------

# MAGIC %md
# MAGIC An even better option is to use the tools already built-in to Spark.  The MLlib guide has a lot of information regarding [evaluation metrics](http://spark.apache.org/docs/latest/mllib-evaluation-metrics.html).  For ML, you can find details in the [Python](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.evaluation) and [Scala](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.evaluation.package) APIs.
# MAGIC  
# MAGIC A common metric used for logistic regression is area under the ROC curve (AUC).  We can use the `BinaryClasssificationEvaluator` to obtain the AUC for our two models.  Make sure to set the metric to "areaUnderROC" and that you set the rawPrediction column to "rawPrediction".
# MAGIC  
# MAGIC Recall that `irisTestPredictions` are the test predictions from our first model and `irisTestPredictions2` are the test predictions from our second model.

# COMMAND ----------

# ANSWER
from pyspark.ml.evaluation import BinaryClassificationEvaluator

binaryEvaluator = (BinaryClassificationEvaluator()
                   .setRawPredictionCol('rawPrediction')
                   .setMetricName('areaUnderROC'))

firstModelTestAUC = binaryEvaluator.evaluate(irisTestPredictions)
secondModelTestAUC = binaryEvaluator.evaluate(irisTestPredictions2)

print 'First model AUC: {0:.3f}'.format(firstModelTestAUC)
print 'Second model AUC: {0:.3f}'.format(secondModelTestAUC)

irisTrainPredictions = pipelineModelLR.transform(irisTrain)
irisTrainPredictions2 = pipelineModelLR2.transform(irisTrain)

firstModelTrainAUC = binaryEvaluator.evaluate(irisTrainPredictions)
secondModelTrainAUC = binaryEvaluator.evaluate(irisTrainPredictions2)

print '\nFirst model training AUC: {0:.3f}'.format(firstModelTrainAUC)
print 'Second model training AUC: {0:.3f}'.format(secondModelTrainAUC)

# COMMAND ----------

# TEST
Test.assertTrue(firstModelTestAUC > .95, 'incorrect firstModelTestAUC')
Test.assertTrue(secondModelTrainAUC > .95, 'incorrect secondModelTrainAUC')

# COMMAND ----------

# MAGIC %md
# MAGIC **Visualization: ROC curve **
# MAGIC  
# MAGIC We will now visualize how well the model predicts our target.  To do this we generate a plot of the ROC curve.  The ROC curve shows us the trade-off between the false positive rate and true positive rate, as we liberalize the threshold required to predict a positive outcome.  A random model is represented by the dashed line.

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

def generateROC(ax, labelsAndScores):
    labelsAndWeights = labelsAndScores.collect()
    labelsAndWeights.sort(key=lambda (k, v): v, reverse=True)
    labelsByWeight = np.array([0.0] + [k for (k, v) in labelsAndWeights])

    length = labelsByWeight.size - 1
    truePositives = labelsByWeight.cumsum()
    numPositive = truePositives[-1]
    falsePositives = np.arange(0.0, length + 1, 1.) - truePositives

    truePositiveRate = truePositives / numPositive
    falsePositiveRate = falsePositives / (length - numPositive)

    # Generate layout and plot data
    ax.set_xlim(-.05, 1.05), ax.set_ylim(-.05, 1.05)
    ax.set_ylabel('True Positive Rate (Sensitivity)')
    ax.set_xlabel('False Positive Rate (1 - Specificity)')
    ax.plot(falsePositiveRate, truePositiveRate, color='red', linestyle='-', linewidth=3.)
    ax.plot((0., 1.), (0., 1.), linestyle='--', color='orange', linewidth=2.)  # Baseline model


labelsAndScores = (irisTestPredictions
                   .select('label', 'rawPrediction')
                   .rdd
                   .map(lambda r: (r[0], r[1][1])))

labelsAndScores2 = (irisTestPredictions2
                    .select('label', 'rawPrediction')
                    .rdd
                    .map(lambda r: (r[0], r[1][1])))

fig, axList = prepareSubplot(np.arange(0., 1.1, 0.1), np.arange(0., 1.1, 0.1), figsize=(12., 5.), subplots=(1,2))
ax0, ax1 = axList
ax0.set_title('First Model', color='#999999')
ax1.set_title('Second Model', color='#999999')
generateROC(axList[0], labelsAndScores)
generateROC(axList[1], labelsAndScores2)
display(fig)

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

metric = 'precision'

multiclassEval = MulticlassClassificationEvaluator()

multiclassEval.setMetricName(metric)
print 'Model one {0}: {1:.3f}'.format(metric, multiclassEval.evaluate(irisTestPredictions))
print 'Model two {0}: {1:.3f}\n'.format(metric, multiclassEval.evaluate(irisTestPredictions2))

# COMMAND ----------

import inspect
print inspect.getsource(MulticlassClassificationEvaluator)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using MLlib instead of ML
# MAGIC  
# MAGIC We've been using `ml` transformers, estimators, pipelines, and evaluators.  How can we accomplish the same things with MLlib?

# COMMAND ----------

irisTestPredictions.columns

# COMMAND ----------

irisTestPredictions.take(1)

# COMMAND ----------

# MAGIC %md
# MAGIC Pull the data that we need from our `DataFrame` and create `BinaryClassificationMetrics`.

# COMMAND ----------

from pyspark.mllib.evaluation import BinaryClassificationMetrics

modelOnePredictionLabel = (irisTestPredictions
                           .select('rawPrediction', 'label')
                           .rdd
                           .map(lambda r: (float(r[0][1]), r[1])))

modelTwoPredictionLabel = (irisTestPredictions2
                           .select('rawPrediction', 'label')
                           .rdd
                           .map(lambda r: (float(r[0][1]), r[1])))

metricsOne = BinaryClassificationMetrics(modelOnePredictionLabel)
metricsTwo = BinaryClassificationMetrics(modelTwoPredictionLabel)

print metricsOne.areaUnderROC
print metricsTwo.areaUnderROC

# COMMAND ----------

# MAGIC %md
# MAGIC To build a logistic regression model with MLlib we'll need the data to be an RDD of `LabeledPoints`.  For testing purposes we'll pull out the label and features into a tuple, since we'll want to make predictions directly on the features and not on a `LabeledPoint`.

# COMMAND ----------

from pyspark.mllib.regression import LabeledPoint

irisTrainRDD = (irisTrainPredictions
                .select('label', 'featuresBucketized')
                .map(lambda r: LabeledPoint(r[0], r[1]))
                .cache())

irisTestRDD = (irisTestPredictions
               .select('label', 'featuresBucketized')
               .map(lambda r: (r[0], r[1]))
               .cache())

print irisTrainRDD.take(2)
print irisTestRDD.take(2)

# COMMAND ----------

# MAGIC %md
# MAGIC Now, we can use MLlib's logistic regression on our `RDD` of `LabeledPoints`.  Note that we'll use `LogisticRegressionWithLBFGS` as it tends to converge faster than `LogisticRegressionWithSGD`.

# COMMAND ----------

from pyspark.mllib.classification import LogisticRegressionWithLBFGS
help(LogisticRegressionWithLBFGS)

# COMMAND ----------

mllibModel = LogisticRegressionWithLBFGS.train(irisTrainRDD, iterations=1000, regParam=0.0)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's calculate our accuracy using `RDDs`.

# COMMAND ----------

rddPredictions = mllibModel.predict(irisTestRDD.values())
predictAndLabels = rddPredictions.zip(irisTestRDD.keys())

mllibAccuracy = predictAndLabels.map(lambda (p, l): p == l).mean()
print 'MLlib model accuracy: {0:.3f}'.format(mllibAccuracy)

