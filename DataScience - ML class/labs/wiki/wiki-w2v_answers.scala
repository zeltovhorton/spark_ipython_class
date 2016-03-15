// Databricks notebook source exported at Tue, 16 Feb 2016 19:38:44 UTC
// MAGIC %md
// MAGIC <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-nd/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-nd/4.0/">Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International License</a>.

// COMMAND ----------

// MAGIC %md
// MAGIC # Wikipedia: Word2Vec
// MAGIC  
// MAGIC In this lab, we'll use `Word2Vec` to create vectors the words found in the Wikipedia dataset.  We'll use `Word2Vec` by passing in a `DataFrame` containing sentences.  We can pass into `Word2Vec` what length of vector to create, with larger vectors taking more time to build.
// MAGIC  
// MAGIC Be able to convert words into vectors provides us with features that can be used in traditional machine learning algorithms.  These vectors can be used to compare word similarity, sentence similarity, or even larger sections of text.

// COMMAND ----------

// MAGIC %md
// MAGIC Load the data.

// COMMAND ----------

val baseDir = "/mnt/ml-class/"
val dfSmall = sqlContext.read.parquet(baseDir + "smallwiki.parquet")

// COMMAND ----------

dfSmall.count

// COMMAND ----------

// MAGIC %md
// MAGIC Filter out unwanted data.

// COMMAND ----------

import org.apache.spark.sql.{functions => func}
import org.apache.spark.sql.functions.col
 
val filtered = dfSmall.filter(($"title" !== "<PARSE ERROR>") &&
                              $"redirect_title".isNull &&
                              $"text".isNotNull)

// COMMAND ----------

// MAGIC %md
// MAGIC Change all text to lower case.

// COMMAND ----------

val lowered = filtered.select($"*", func.lower($"text").as("lowerText"))

// COMMAND ----------

val parsed = lowered.drop("text").withColumnRenamed("lowerText", "text")

// COMMAND ----------

// MAGIC %md
// MAGIC Split the Wikipedia text into sentences.

// COMMAND ----------

val pattern = """(\. |\n{2,})"""
 
val matches = pattern.r.findAllIn("Wiki page. *More information*\n\n And a line\n that continues.")
 
matches.toArray

// COMMAND ----------

import org.apache.spark.ml.feature.RegexTokenizer
 
val tokenizer = new RegexTokenizer()
  .setInputCol("text")
  .setOutputCol("sentences")
  .setPattern(pattern)
 
val sentences = tokenizer.transform(parsed).select("sentences")
display(sentences)

// COMMAND ----------

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import scala.collection.mutable.WrappedArray
 
val sentenceRDD = sentences
  .flatMap(x => x.getAs[WrappedArray[String]](0))
  .map(Row(_))
 
val sentenceSchema = StructType(Array(StructField("sentence", StringType)))
val sentence = sqlContext.createDataFrame(sentenceRDD, sentenceSchema)
 
display(sentence)

// COMMAND ----------

// MAGIC %md
// MAGIC Split the sentences into words.

// COMMAND ----------

val tokenizerWord = new RegexTokenizer()
  .setInputCol("sentence")
  .setOutputCol("words")
  .setPattern("""\W+""")
 
val words = tokenizerWord.transform(sentence).select("words")
display(words)

// COMMAND ----------

// MAGIC %md
// MAGIC Use our `removeWords` function that we registered in wiki-eda to clean up stop words.

// COMMAND ----------

sqlContext.sql("drop table if exists words")
words.registerTempTable("words")

// COMMAND ----------

val noStopWords = sqlContext.sql("select removeWords(words) as words from words")
display(noStopWords)

// COMMAND ----------

val wordVecInput = noStopWords.filter(func.size($"words") !== 0)
wordVecInput.count

// COMMAND ----------

// MAGIC %md
// MAGIC Build the `Word2Vec` model.  This take about a minute with two workers.

// COMMAND ----------

import org.apache.spark.ml.feature.Word2Vec
val word2Vec = new Word2Vec()
  .setVectorSize(150)
  .setMinCount(50)
  .setInputCol("words")
  .setOutputCol("result")
  .setSeed(0)
val model = word2Vec.fit(wordVecInput)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's see the model in action.

// COMMAND ----------

model
  .findSynonyms("house", 10)
  .collect()
  .foreach(println)

// COMMAND ----------

val synonyms = model.findSynonyms("fruit", 10).collect()
synonyms.foreach(println)

// COMMAND ----------

model
  .findSynonyms("soccer", 10)
  .collect()
  .foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC How can we calculate similarity between vectors and handle creating a vector for multiple words at once?

// COMMAND ----------

val tmpDF = sqlContext.createDataFrame(Seq(Tuple1[Array[String]](Array("fruit")),
                                           Tuple1[Array[String]](Array("flower")),
                                           Tuple1[Array[String]](Array("fruit", "flower")))).toDF("words")
display(tmpDF)

// COMMAND ----------

import org.apache.spark.mllib.linalg.DenseVector
val vFruit = model.transform(tmpDF).map(r => r.getAs[DenseVector](r.size - 1)).collect()

// COMMAND ----------

// MAGIC %md
// MAGIC Let's create a cosine similarity measure.

// COMMAND ----------

import com.github.fommil.netlib.F2jBLAS
val f2j = new F2jBLAS
 
def dot(x: DenseVector, y: DenseVector) = f2j.ddot(x.size, x.values, 1, y.values, 1)
def norm(x: DenseVector) = f2j.dnrm2(x.size, x.values, 1)
def similarity(x: DenseVector, y: DenseVector) = dot(x, y) / (norm(x) * norm(y))
 
val v0 = vFruit(0)
val v1 = vFruit(1)
val v2 = vFruit(2)
 
println(similarity(v0, v1))
println(similarity(v1, v2))

// COMMAND ----------

// MAGIC %md
// MAGIC `Word2Vec` handles multiple words by averaging the vectors.

// COMMAND ----------

import runtime.ScalaRunTime.stringOf
println(stringOf(v0.toArray.slice(0,6)))
println(stringOf(v1.toArray.slice(0,6)))
val v0v1 = v0.toArray.zip(v1.toArray).map(x => (x._1 + x._2) / 2.0)
println(stringOf(v0v1.slice(0,6))) // Averaging the word vectors gives us the vector for both words in a sentence
println(stringOf(v2.toArray.slice(0,6)))
println("\n\n")

// COMMAND ----------

def T1 = Tuple1[Array[String]] _
 
val tmpDF = sqlContext.createDataFrame(Seq(T1(Array("king")),
                                           T1(Array("man")),
                                           T1(Array("woman")),
                                           T1(Array("queen")))).toDF("words")
val v1 = model.transform(tmpDF).map(r => r.getAs[DenseVector](r.size - 1)).collect()
display(tmpDF)

// COMMAND ----------

import org.apache.spark.mllib.linalg.Vectors
val Array(k, m, w, q) = v1
val kmw = Vectors.dense(k.toArray.zip(m.toArray).map(x => x._1 - x._2).zip(w.toArray).map(x => x._1 + x._2))
  .asInstanceOf[DenseVector]
 
println(similarity(k, q))
println(similarity(k, m))
println(similarity(q, m))
println(similarity(q, w))
println(similarity(q, kmw))

// COMMAND ----------

// MAGIC %md
// MAGIC Load a model trained on more data and view the differences.

// COMMAND ----------

import org.apache.spark.mllib.feature.Word2VecModel
val modelOne = Word2VecModel.load(sc, "/mnt/ml-class/one.w2v")

// COMMAND ----------

// MAGIC %md
// MAGIC How do our two models compare?

// COMMAND ----------

def compareModels(word:String) = {
  val mOne = modelOne.findSynonyms(word, 10)
  val m = model.findSynonyms(word, 10).map(r => (r.getAs[String](0), r.getAs[Double](1))).collect()
  mOne.zip(m).foreach(println)
}
 
compareModels("soccer")

// COMMAND ----------

compareModels("fruit")

// COMMAND ----------

// MAGIC %md
// MAGIC Any guesses on what the top results will be?

// COMMAND ----------

compareModels("apple")

// COMMAND ----------

// MAGIC %md
// MAGIC How would find similarities for two words at once?

// COMMAND ----------

def vAvg(v1: Array[Float], v2: Array[Float]) =
  v1.zip(v2).map(x => (x._1 + x._2) / 2.0)
def vAdd(v1: Array[Double], v2: Array[Double]) =
  v1.zip(v2).map(x => (x._1 + x._2))
def vSub(v1: Array[Double], v2: Array[Double]) =
  v1.zip(v2).map(x => (x._1 - x._2))
 
val Array(chinese, river) = Array("chinese", "river").map(modelOne.getVectors)
 
modelOne.findSynonyms(Vectors.dense(vAvg(chinese, river)), 5).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC Note that neither "chinese" or "river" captures "Yangtze" in the first five.

// COMMAND ----------

modelOne
  .findSynonyms("chinese", 5)
  .zip(modelOne.findSynonyms("river", 5))
  .foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's try our vector math again.

// COMMAND ----------

import org.apache.spark.mllib.linalg.{Vectors, DenseVector}
 
val kmwq = Array("king", "man", "woman", "queen")
  .map(x => modelOne.getVectors(x).map(_.toDouble))
  .map(x => Vectors.dense(x).asInstanceOf[DenseVector])
 
val Array(k, m, w, q) = kmwq
val kmw = Vectors.dense(k.toArray.zip(m.toArray).map(x => x._1 - x._2).zip(w.toArray).map(x => x._1 + x._2))
  .asInstanceOf[DenseVector]
 
println(similarity(k, q))
println(similarity(k, m))
println(similarity(q, m))
println(similarity(q, w))
println(similarity(q, kmw))
