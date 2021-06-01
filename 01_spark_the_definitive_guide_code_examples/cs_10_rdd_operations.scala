// Databricks notebook source
// create a dummy RDD
spark.range(500).rdd

// COMMAND ----------
// convert RDD to a DF
spark.range(500).rdd.toDF().show(2)

// COMMAND ----------

// making RDD from textFile path
spark.sparkContext.textFile("/some/path/withTextFiles")


// COMMAND ----------

// creating custom RDD
val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
.split(" ")
val words = spark.sparkContext.parallelize(myCollection, 2)


// COMMAND ----------

// RDD transformations
// distinct
words.distinct().count()

// COMMAND ----------

words.take(4) // take() alternate of show()?

// COMMAND ----------


// Map on RDDS
val words2 = words.map(word => (word, word.startsWith("S")))
words2.take(3)

// RDD operations: filter
words2.filter(word => (word._2)).take(3)

// COMMAND ----------

// Checkpointing for RDDs
spark.sparkContext.setCheckpointDir("/some/path/for/checkpointing")
words.checkpoint()

// COMMAND ----------

val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
.split(" ")
val words = spark.sparkContext.parallelize(myCollection, 2)


// Convert RDD into a key-val pair for more operations
val words2 = words.map(word => (word.toLowerCase, 1))
words2.take(3)

// COMMAND ----------

val words3 = words.keyBy(word => 1)
words3.take(3)
words3.mapValues(value => value.toUpperCase).collect()

// COMMAND ----------

words3.flatMapValues(value => value.toUpperCase).collect()

// COMMAND ----------

words3.keys.collect()
words3.values.collect()

// COMMAND ----------

words3.lookup(1) // to read value from a key


