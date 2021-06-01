// Databricks notebook source
val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
.split(" ")
val words = spark.sparkContext.parallelize(myCollection, 2)

// COMMAND ----------

// Key Value methods, all ByKey methods require PairRDD type to execute the operation on em
// 
words.map(word => (word.toLowerCase, 1)).take(3)
// is similar to
words.keyBy(word => 1).take(3)

// COMMAND ----------

val keyword = words.keyBy(word => word.toLowerCase.toSeq(0).toString)
// mapValues, to iterate over value
keyword.mapValues(word => word.toUpperCase).collect()

// COMMAND ----------

// alternative is flatMapValues, to run this on every characater
keyword.flatMapValues(word => word.toUpperCase).collect()

// COMMAND ----------

val chars = words.flatMap(word => word.toLowerCase.toSeq) /*['s', 'p', 'a']..*/
val KVcharacters = chars.map(letter => (letter, 1)) // map to a key val pair ('s',1)
def maxFunc(left:Int, right:Int) = math.max(left, right) // function to return max of 2 values
def addFunc(left:Int, right:Int) = left + right // function to add 2 values
val nums = sc.parallelize(1 to 30, 5) // array of 30 numbers


// COMMAND ----------

// countByKey
KVcharacters.countByKey()
val timeout = 1000L
val confidence = 0.95
KVcharacters.countByKeyApprox(timeout, confidence)


// COMMAND ----------

KVcharacters.collect()

// COMMAND ----------

// one way -  groupByKey
KVcharacters.groupByKey().collect() // (d, (1,1,1,1)), (p (1,1,1))
// KVcharacters.groupByKey().map(row => (row._1, row._2.reduce(addFunc))).collect()

// COMMAND ----------

KVcharacters.reduceByKey(addFunc).collect() // better approach, less load on cluster, doesnt have to hold all value in memory

// COMMAND ----------

nums.aggregate(0)(maxFunc, addFunc)
// combineByKey, aggregateByKey, foldByKey

// COMMAND ----------

// Inner join in RDDs
val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct
import scala.util.Random
val keyedChars = distinctChars.map(c => (c, new Random().nextDouble()))
val outputPartitions = 10
KVcharacters.join(keyedChars).count() // difference between this and
KVcharacters.join(keyedChars, outputPartitions).count() // this ???


// COMMAND ----------

val numRange = sc.parallelize(0 to 9, 2)
words.zip(numRange).collect()


// COMMAND ----------

// TO STUDY: coalesce and repartition for RDDs [238]

// COMMAND ----------


