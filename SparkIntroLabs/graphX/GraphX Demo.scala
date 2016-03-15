// Databricks notebook source exported at Thu, 5 Nov 2015 21:08:19 UTC
// MAGIC %md #GraphX Demonstration

// COMMAND ----------

// MAGIC %md ## What is GraphX?
// MAGIC 
// MAGIC [GraphX](http://spark.apache.org/docs/latest/graphx-programming-guide.html#the-property-graph) is an API that allows you to analyze graph data (social graphs, network graphs, and the like) using the power of the Spark cluster.
// MAGIC 
// MAGIC This notebook contains a simple demonstration of just one small part of the GraphX API. It's designed to give you a bird's eye view of GraphX. Hopefully, it'll whet your appetite.

// COMMAND ----------

// MAGIC %md ## Resources
// MAGIC 
// MAGIC * [Manning](http://www.manning.com/) has an early-access (MEAP) [_Spark GraphX in Action_](http://www.manning.com/malak/) book.
// MAGIC * The [GraphX Programming Guide](http://spark.apache.org/docs/latest/graphx-programming-guide.html#the-property-graph) is a good place to start.
// MAGIC * The paper "[GraphX: A Resilient Distributed Graph System on Spark](https://amplab.cs.berkeley.edu/wp-content/uploads/2013/05/grades-graphx_with_fonts.pdf)" is worth reading.

// COMMAND ----------

// MAGIC %md A variant of this demonstration, with a detailed explanation, can be found here: <http://ampcamp.berkeley.edu/big-data-mini-course/graph-analytics-with-graphx.html>

// COMMAND ----------

// MAGIC %md The following example creates a _property graph_. (See <http://spark.apache.org/docs/latest/graphx-programming-guide.html#the-property-graph>) Let's assume we're dealing with the relationship (or _level of interest_) graph for a dating site. Let's create a graph of some of the participants and their interest levels in one another.
// MAGIC 
// MAGIC Here's an example of a property graph:
// MAGIC 
// MAGIC ![Property Graph Example](http://i.imgur.com/C4FPxXV.png)

// COMMAND ----------

import org.apache.spark.graphx._  // Edge comes from here
import org.apache.spark.rdd.RDD

case class Person(id: Int, name: String, age: Int)

type VertexType = Tuple2[Long, Person]

val vertexArray = Array[VertexType](
  /* Index number, Person */
  (1L, Person(1, "Angelica", 23)), // Tuple2[Long, Person] or VertexType
  (2L, Person(2, "Valentino", 31)),
  (3L, Person(3, "Mercedes", 52)),
  (4L, Person(4, "Anthony", 39)),
  (5L, Person(5, "Roberto", 45))
)

val edgeArray = Array(
  /* First Person by Index >> related somehow to Second Person by Index, with Attribute Value (which is contextual) */
  Edge(2L, 1L, 9), // Valentino (source) is highly attracted to Angelica (target) (7 stars out of 10)
  Edge(2L, 4L, 2), // Valentino is not really interested in Anthony (only 2 stars)
  Edge(3L, 2L, 8), // Mercedes is EXTREMELY interested in Valentino (8 stars!)
  Edge(3L, 5L, 3), // Mercedes is not all that interested in Roberto (3 stars)
  Edge(4L, 1L, 1), // Anthony isn't the least bit interested in Angelica, but gave her 1 star anyway (not to hurt her feelings) ...
  Edge(5L, 3L, 5), // Roberto is ambivalent whether he's interested in Mercedes (5/10 stars)
  Edge(5L, 1L, 7), // Roberto is attracted to Angelica
  Edge(1L, 2L, 9), // Angelica is highly attracted to Valentino
  Edge(4L, 3L, 0)  // Anthony can't stand Mercedes
  
)

// Vertices are NODES on the (social) graph
val vertexRDD: RDD[VertexType] = sc.parallelize(vertexArray)

// Edges are "arrows" direction (going from one node towards another node)
val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

val g: Graph[Person, Int] = Graph(vertexRDD, edgeRDD)

val results = g.triplets.filter(t => t.attr >= 8).filter(t => t.srcAttr.age > 40)

for (triplet <- results.collect) {
  println(s"*** ${triplet.srcAttr.name} is extremely attracted to ${triplet.dstAttr.name} \n")
}


// COMMAND ----------

for (t <- g.triplets.collect()) { //Collection of Vertex >> Edge >> Vertex ... walking the Graph here.
  // We're looking at one (vertex, edge, vertex) triplet here.
  t.attr match { // match on rating, for each element in the Graph assign a value to t
    case n if (n > 8)  => println(s"${t.srcAttr.name} is in love with ${t.dstAttr.name}: $n stars")
    case n if (n > 6)  => println(s"${t.srcAttr.name} is attracted to ${t.dstAttr.name}: $n stars")
    case n if (n > 3)  => println(s"${t.srcAttr.name} is ambivalent about ${t.dstAttr.name}: $n stars" )
    case n if (n > 1)  => println(s"${t.srcAttr.name} is not attracted to ${t.dstAttr.name}: $n stars")
    case n if (n == 1) => println(s"${t.srcAttr.name} is not attracted to ${t.dstAttr.name}: 1 star")
    case _             => println(s"${t.srcAttr.name} can't stand ${t.dstAttr.name}: NO STARS")
  }
}
println()

// COMMAND ----------

// MAGIC %md We can visually scan the output to find people who are in love with each other, but only because the graph is so small. In a larger graph, that would be tedious. So, let's see if we can find process the graph programmatically to find people who are in love. We'll define "in love" as two people who have rated each with more than 8 stars.
// MAGIC 
// MAGIC This is a little complicated. (There may well be a better way to do this.)

// COMMAND ----------

// First, get the graph's triplets. (Recall: Each trip is a pair of vertices
// and the edge that connects them.)

val rdd1 = g.triplets
            // Get rid of any pair with a score of 8 or lower.
            .filter { t => t.attr > 8 }
            // Map each triplet into into a (unique-key, (person, person))
            // tuple. We need the unique key, because cogroup() wants a
            // key/value pair for cogroup().
            .map { t =>
              val pair = (t.srcAttr, t.dstAttr)
              (pair.hashCode, pair)
            }

// Now, create a second RDD which is a mirror of this RDD (i.e., with the
// people in each pair flipped). We can then join the two RDDs using cogroup(),
// to find pairs where (person1 likes person2) AND (person2 likes person1).
//
// For example, suppose we have:
//   [ (Joe, Rita),
//     (Rita, Joe),
//     (Jamie, Bill) ]
//
// We want to detect that (Joe likes Rita) AND (Rita likes Joe).
// To do that, we can create a second RDD that looks like this:
//
//   [ (Rita, Joe),
//     (Joe, Rita),
//     (Bill, Jamie) ]
//
// Then, we can use cogroup() to match them up.
val rdd2 = rdd1.map { case (hash, (p1, p2)) => (hash, (p2, p1)) }

// Use cogroup() to join the two RDDs.
val joined = rdd1.cogroup(rdd2)

// The joined RDD consists of the unique ID, a left iterable and a right iterable.
// Just looping over one of the iterables is good enough for our purposes. Note that
// we'll get two hits for each pair.
for { (hash, (leftIterable, rightIterable)) <- joined.collect
      (person1, person2)                    <- leftIterable }
  println(s"${person1.name} \u2661 ${person2.name}")


// COMMAND ----------

// MAGIC %md If `cogroup()` is confusing, you can use DataFrames.

// COMMAND ----------

val df = rdd1.toDF("id", "pair")
df.printSchema()


// COMMAND ----------

// MAGIC %md Let's do a self-join. We'll create two separate derived DataFrames with renamed columns.

// COMMAND ----------

val dfA = df.select($"pair".as("pair1"))
val dfB = df.select($"pair".as("pair2"))
dfA.show()
dfB.show()


// COMMAND ----------

// MAGIC %md Perfect! Now we can do a simple DataFrame join.

// COMMAND ----------

dfA.join(dfB, $"pair1._1.id" === $"pair2._2.id").select($"pair1._1.name", $"pair1._2.name").collect().foreach { row =>
  val name1 = row(0)
  val name2 = row(1)
  println(s"$name1 \u2661 $name2")
}

// COMMAND ----------

// MAGIC %md Either way...
// MAGIC # We have achieved mutual compatibility
// MAGIC ![Angelica](http://i.imgur.com/JtUvZfC.jpg) ![Heart](http://i.imgur.com/9kNn5lK.png) ![Valentino](http://i.imgur.com/ZBB8Hlr.jpg)
