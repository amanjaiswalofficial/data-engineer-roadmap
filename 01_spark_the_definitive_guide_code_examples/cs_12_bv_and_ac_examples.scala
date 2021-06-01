// Databricks notebook source
val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
.split(" ")
val words = spark.sparkContext.parallelize(myCollection, 2)

// COMMAND ----------

val supplementalData = Map("Spark" -> 1000, "Definitive" -> 200,
"Big" -> -300, "Simple" -> 100)

// COMMAND ----------

// Broadcasting a map over the set of clusters for reusability purposes
val supBroadcast = spark.sparkContext.broadcast(supplementalData)
// see the broadcast var's value
supBroadcast.value


// COMMAND ----------

// Accessing broadcast var's value to use
val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
.split(" ")
val words = spark.sparkContext.parallelize(myCollection, 2)
words.map(word => (word, supBroadcast.value.getOrElse(word, -1))).sortBy(wordPair => wordPair._2).collect()

// COMMAND ----------

// Accumulators in Spark
case class Flight(DEST_COUNTRY_NAME: String,
ORIGIN_COUNTRY_NAME: String, count: BigInt)
val flights = spark.read
.parquet("/FileStore/tables/2010-summary.parquet")
.as[Flight]

// COMMAND ----------

import org.apache.spark.util.LongAccumulator
val accUnnamed = new LongAccumulator // creating an unnamed accumulator
val acc = spark.sparkContext.register(accUnnamed)

val accChina2 = spark.sparkContext.longAccumulator("China") // creating a named accumulator, shorthand method
val accChina = new LongAccumulator
spark.sparkContext.register(accChina, "China") // creating a named accumulator, longHand

// COMMAND ----------

def accChinaFunc(flight_row: Flight) = {
val destination = flight_row.DEST_COUNTRY_NAME
val origin = flight_row.ORIGIN_COUNTRY_NAME
if (destination == "China") {
accChina.add(flight_row.count.toLong) // updating accumulator's value, which is universal to all the clusters
}
if (origin == "China") {
accChina.add(flight_row.count.toLong) // updating accumulator's value, which is universal to all the clusters
}
}

// COMMAND ----------

flights.foreach(flight_row => accChinaFunc(flight_row))

// COMMAND ----------

// get the new value of accumulators
accChina.value

// COMMAND ----------


