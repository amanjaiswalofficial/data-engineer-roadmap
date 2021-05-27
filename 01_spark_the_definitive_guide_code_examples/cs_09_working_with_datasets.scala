// Databricks notebook source
/*WORKING WITH DATASETS*/

// defining a dummy class for loading datasets
case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt)

// COMMAND ----------

val flightsDF = spark.read
.parquet("./tables/2010-summary.parquet/")

// loading file's data as dataset
val flights = flightsDF.as[Flight]

// COMMAND ----------

// actions like show(), collect()
flights.show(2)

// COMMAND ----------

// Access a specific value from dataset
flights.first.DEST_COUNTRY_NAME

// COMMAND ----------

// Writing a function to be applied on each row of dataset, hence will work with some iterating technique
// like filter
def originIsDestination(flight_row: Flight): Boolean = {
  return flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
}

flights.filter(flight_row => originIsDestination(flight_row)).show()

// COMMAND ----------

/*Mapping*/
val destinations = flights.map(f => f.DEST_COUNTRY_NAME)
destinations.show(3)

// or
val localDestinations = destinations.take(5)
localDestinations // is an array


// COMMAND ----------

// Join

case class FlightMeta(count: BigInt, randomData: BigInt)

val flightsMeta = spark.range(500).map(x => (x, scala.util.Random.nextLong))
.withColumnRenamed("_1", "count").withColumnRenamed("_2", "randomData").as[FlightMeta]

val flights2 = flights.joinWith(flightsMeta, flights.col("count") === flights.col("count"))

flights2.show(3)

// COMMAND ----------

flights2.selectExpr("_1.DEST_COUNTRY_NAME").show(2)

// COMMAND ----------

// Can also join dataset and dataframes
val flights2 = flights.join(flightsMeta.toDF(), Seq("count")) // returns a DF, hence lost type info

// COMMAND ----------

flights2.show(2)

// COMMAND ----------

// Grouping data
flights.groupByKey(x => x.DEST_COUNTRY_NAME).count().show(5)

// COMMAND ----------

// Applying functions over grouped data
def grpSum(name: String, values: Iterator[Flight]) = {
  values.dropWhile(_.count > 2).map(x => (name, x))
}

flights.groupByKey(x => x.DEST_COUNTRY_NAME).flatMapGroups(grpSum).show(5)

// COMMAND ----------


