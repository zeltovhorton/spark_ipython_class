// Databricks notebook source exported at Tue, 16 Feb 2016 19:40:11 UTC
// MAGIC %md
// MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

// COMMAND ----------

// MAGIC %md
// MAGIC # Wikipedia: LDA
// MAGIC  
// MAGIC This lab explores building a Latent Dirichlet allocation (LDA) model.  We'll use LDA to generate 10 topics that correspond to the Wikipedia data.  These topics will correspond to words found in the articles.  We'll take an article and see which of the topics it is associated with.  LDA won't just categories the article into one category but will give a numeric value that corresponds to the articles relevance to each of the 10 topics.
// MAGIC  
// MAGIC LDA is currently only implemented in MLlib.  Details can be found in the [MLlib clustering guide](http://spark.apache.org/docs/latest/mllib-clustering.html#latent-dirichlet-allocation-lda).
// MAGIC  
// MAGIC Additional details about the algorithm can be found on [Wikipedia](http://spark.apache.org/docs/latest/mllib-clustering.html#latent-dirichlet-allocation-lda).

// COMMAND ----------

// MAGIC %md
// MAGIC #### Load in the data.
// MAGIC  
// MAGIC First, we'll load the `DataFrame` which was called `noStopWords` in the wiki-etl-eda notebook.  This way we can avoid some of the pre-processing steps.

// COMMAND ----------

val noStopWords = sqlContext.read.parquet("/mnt/ml-class/oneWords.parquet").cache // noStopWords from wiki-etl-eda

// COMMAND ----------

// MAGIC %md
// MAGIC Recall that even though we removed a series of stop words, the word counts for certain works were very high and they were words that didn't seem to convey much meaning for an article.  We'll view words ordered by their frequency to find a cutoff point for removing additional stop words.

// COMMAND ----------

import org.apache.spark.sql.functions.{explode, count, desc}
val wordCounts = noStopWords
  .select(explode($"words").as("word"))
  .groupBy("word")
  .agg(count($"word").alias("counts"))
  .orderBy(desc("counts"))
display(wordCounts)

// COMMAND ----------

// MAGIC %md
// MAGIC Create a new list of stop words based on our cutoff.

// COMMAND ----------

val newStopWords = wordCounts.filter($"counts" > 55700).select("word").map(_.getAs[String](0)).collect() // 3000 small, 55700 one, all 5500000
println(newStopWords)

// COMMAND ----------

// MAGIC %md
// MAGIC Use `StopWordsRemover` to remove our new list of stop words from the dataset.

// COMMAND ----------

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.functions.size
 
val sw = new StopWordsRemover()
  .setInputCol("words")
  .setOutputCol("fewerWords")
  .setStopWords(newStopWords)
 
val updatedStopWords = sw.transform(noStopWords).filter(size($"fewerWords") > 0)

// COMMAND ----------

// MAGIC %md
// MAGIC Next, review a few examples to see if we have improved our word list.

// COMMAND ----------

display(updatedStopWords.select("fewerWords"))

// COMMAND ----------

// MAGIC %md
// MAGIC Then, we'll use `CountVectorizer` to obtain word counts per article.  Note that these will be stored as `SparseVectors` within our `DataFrame`.

// COMMAND ----------

import org.apache.spark.ml.feature.CountVectorizer
val cv = new CountVectorizer()
println(cv.explainParams)

// COMMAND ----------

cv
  .setInputCol("fewerWords")
  .setOutputCol("counts")
 
val cvModel = cv.fit(updatedStopWords)
val wordCounts = cvModel.transform(updatedStopWords)

// COMMAND ----------

// MAGIC %md
// MAGIC We'll save our vocabulary so that we can skip some steps if we want to use the LDA model later.

// COMMAND ----------

val cvVocabRDD = sc.parallelize(cvModel.vocabulary)
//cvVocabRDD.saveAsTextFile("/mnt/ml-class/oneCVVocab.txt")

// COMMAND ----------

// MAGIC %md
// MAGIC This is how we can reload in the vocabulary from a text file.

// COMMAND ----------

val cvVocabRDD = sc.textFile("/mnt/ml-class/oneCVVocab.txt")
cvVocabRDD.take(3)

// COMMAND ----------

// MAGIC %md
// MAGIC Recall that `CountVectorizer` returns a `SparseVector`.

// COMMAND ----------

display(wordCounts.select("counts"))

// COMMAND ----------

// MAGIC %md
// MAGIC Let's go ahead and build that LDA model.  Since `LDA` falls within `mllib` it doesn't take in a `DataFrame`.  We need to provide an `RDD`.  `LDA` expects an `RDD` that contains a tuple of (index, `Vector`) pairs.  Under the hood LDA uses GraphX which performs shuffles which change the order of the data, so the usual `mllib` strategy of zipping the results together will not work.  With LDA we'll use joins based on the articles indices.

// COMMAND ----------

// 4-5 mins to run w/ 3 nodes
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vector
 
val corpus = wordCounts
  .select("counts", "title")
  .map(r => (r.getAs[Vector](0), r.getAs[String](1)))
  .zipWithIndex
  .map(_.swap)
  .cache
 
val ldaModel = new LDA()
  .setK(10)
  .run(corpus.mapValues(_._1))

// COMMAND ----------

// MAGIC %md
// MAGIC The below command was used to save the LDA model for later use.

// COMMAND ----------

//ldaModel.save(sc, "/mnt/ml-class/oneLDA.model")

// COMMAND ----------

// MAGIC %md
// MAGIC This is how we load back in the saved model.

// COMMAND ----------

import org.apache.spark.mllib.clustering.DistributedLDAModel
val ldaModel = DistributedLDAModel.load(sc, "/mnt/ml-class/oneLDA.model")

// COMMAND ----------

// MAGIC %md
// MAGIC What's stored in an LDA model?

// COMMAND ----------

display(dbutils.fs.ls("/mnt/ml-class/oneLDA.model/"))

// COMMAND ----------

display(dbutils.fs.ls("/mnt/ml-class/oneLDA.model/data/"))

// COMMAND ----------

display(dbutils.fs.ls("/mnt/ml-class/oneLDA.model/data/tokenCounts/"))

// COMMAND ----------

// MAGIC %md
// MAGIC Let's view the first three words that are most relevant for our 10 topics.  The first array references work indices and is their relevance to this topic.

// COMMAND ----------

import runtime.ScalaRunTime.stringOf
ldaModel.describeTopics(3).foreach(l => println(stringOf(l)))

// COMMAND ----------

// MAGIC %md
// MAGIC Let's use our `CountVectorizer` vocabulary to generate more readable topics.  Note that this makes use of LDA's `describeTopics`.

// COMMAND ----------

val indexToWord = cvVocabRDD.zipWithIndex.map(_.swap).collectAsMap
 
def describeTopicsWithWords(num: Int) = {
  val topicIndex = ldaModel.describeTopics(num)
  val withWords = topicIndex.map(topic => topic._1.map(indexToWord(_)))
  withWords
}
 
val topicCategories = describeTopicsWithWords(10).map(_.mkString("-"))
topicCategories.foreach(println)
println("\n\n")

// COMMAND ----------

// MAGIC %md
// MAGIC Now let's view the top documents for our 10 topics.  Note that this makes use of `topDocumentsPerTopic` but joins in titles to make the results more readable.  `topDocumentsPerTopic` is available for `DistributedLDAModels`.

// COMMAND ----------

import org.apache.spark.mllib.clustering.DistributedLDAModel
 
def topDocumentsWithTitle(num: Int) = {
  val topDocs = ldaModel.asInstanceOf[DistributedLDAModel].topDocumentsPerTopic(num)
 
  topDocs
    .map(topic => {
      val ids = topic._1
      val idsRDD = sc.parallelize(ids.zipWithIndex)
      val joined = idsRDD.join(corpus.mapValues(_._2)).collectAsMap
      ids.map(joined(_))
    })
}
 
topDocumentsWithTitle(7).zip(topicCategories).foreach(x => {
  println(s"Next Topic: ${x._2}")
  x._1.foreach(println)
  println()
})

// COMMAND ----------

// MAGIC %md
// MAGIC How many articles are we working with?

// COMMAND ----------

corpus.count

// COMMAND ----------

// MAGIC %md
// MAGIC Next, we'll search for an article based on a keyword and then see how that article is classified into topics.  The below example searches for european football related articles.

// COMMAND ----------

val idTitleDF = corpus.map(x => (x._1, x._2._2)).toDF("id", "title")
display(idTitleDF.select("title", "id").where($"title".like("%UEFA%")))

// COMMAND ----------

// MAGIC %md
// MAGIC The article about Belgium seems interesting.  Let's use id 2880 and see what topics are ranked as most relevant.  Note that this uses `topicDistributions` which is a `DistributedLDAModel` method.

// COMMAND ----------

val topicDist = ldaModel.asInstanceOf[DistributedLDAModel].topicDistributions
 
def getTopicDistForID(id: Int) {
  println("Score\tTopic")
  topicDist
    .filter(_._1 == id)
    .map(_._2.toArray)
    .first // now we are working locally
    .zip(topicCategories)
    .sortWith(_._1 > _._1)
    .foreach(x => println(f"${x._1}%.3f\t${x._2}"))
}
 
getTopicDistForID(2880) //2880

// COMMAND ----------

// MAGIC %md
// MAGIC We'll register our `DataFrame` with ids and titles, so that we can query it from SQL.

// COMMAND ----------

idTitleDF.registerTempTable("idTitle")

// COMMAND ----------

// MAGIC %sql
// MAGIC select title, id from idTitle where title like "%$Query%"

// COMMAND ----------

//women%cycling
getTopicDistForID(27441) //27441
