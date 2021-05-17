// Databricks notebook source
// Custom class created to interpret data
case class Flight(DEST_COUNTRY_NAME: String,
ORIGIN_COUNTRY_NAME: String,
count: BigInt)

// Reading data from a file, into a dataframe
val flightsDF = spark.read.parquet("./tables/2010_summary.parquet")

// Dataset API allows to assign Scala class to the records in a dataframe
val flights = flightsDF.as[Flight] 

// Running operations on dataset
flights
.filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
.map(flight_row => flight_row)
.take(5)

// COMMAND ----------

// Streaming Example - Reading data from a folder containing n files
val staticDataFrame = spark.read.format("csv")
.option("header", "true")
.option("inferSchema", "true")
.load("./tables/retail-data/by-day/*.csv")
staticDataFrame.createOrReplaceTempView("retail_data")
val staticSchema = staticDataFrame.schema

// Using window to peek into the data based on some filters/conditions applied
import org.apache.spark.sql.functions.{window, column, desc, col}
staticDataFrame
.selectExpr(
"CustomerId",
"(UnitPrice * Quantity) as total_cost",
"InvoiceDate")
.groupBy(
col("CustomerId"), window(col("InvoiceDate"), "1 day"))
.sum("total_cost")
.show(5)

// COMMAND ----------

// Similar concept used as previous static stream, here instead use readStream
val streamingDataFrame = spark.readStream
.schema(staticSchema)
.option("maxFilesPerTrigger", 1)
.format("csv")
.option("header", "true")
.load("./tables/retail-data/by-day/*.csv")
streamingDataFrame.isStreaming

// COMMAND ----------

// A Window is created over the readStream
val purchaseByCustomerPerHour = streamingDataFrame
.selectExpr("CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
.groupBy($"CustomerId", window($"InvoiceDate", "1 day"))
.sum("total_cost")

// To actually get the data, an action is required
// In case of streaming data, writeStream writes this data in memory and keeps updating
// on every trigger to get appropriate query response
purchaseByCustomerPerHour.writeStream
.format("memory")
.queryName("customer_purchases")
.outputMode("complete")
.start()

// COMMAND ----------

// Running queries as before to get the output
spark.sql("""
SELECT *
FROM customer_purchases
ORDER BY `sum(total_cost)` DESC
""")
.show(5)
