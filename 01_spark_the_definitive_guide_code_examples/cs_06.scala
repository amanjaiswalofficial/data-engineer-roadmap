// Databricks notebook source
// Aggregation Examples
// Reading data 
val df = spark.read.format("csv")
.option("header", "true")
.option("inferSchema", "true")
.load("./tables/online_retail_dataset.csv")
.coalesce(5)
df.cache()
// Create temp view
df.createOrReplaceTempView("dfTable")

// COMMAND ----------

df.count()

// COMMAND ----------

import org.apache.spark.sql.functions.countDistinct
// get complete count
df.select(countDistinct("StockCode")).show()

// COMMAND ----------

import org.apache.spark.sql.functions.{first, last, min, max, sum}
// get first and last items, min and max
df.select(first("StockCode"), last("StockCode")).show()
df.select(min("Quantity"), max("Quantity")).show()
df.select(sum("Quantity")).show()


// COMMAND ----------

df.groupBy("InvoiceNo", "CustomerId").count().show()

// COMMAND ----------

//df.groupBy("InvoiceNo").count().show()
df.groupBy("InvoiceNo", "CustomerId").count().show()

// COMMAND ----------

import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions._

// Using this results same as
df.groupBy("InvoiceNo").agg(count("Quantity").alias("quan")).show()
// using this, so count() can be used as alternative like this
df.groupBy("InvoiceNo").agg(expr("count(Quantity)")).show()

// COMMAND ----------

// Using grouping functions as maps on data
// TO READ
// Window functions
import org.apache.spark.sql.functions.{col, to_date}

// Create a new view of data, including date
val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy HH:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")

// 1. create a window over actual data, here added additional date type over data named as 'date'
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col

val windowSpec = Window.partitionBy("CustomerId", "date")
.orderBy(col("Quantity").desc)
.rowsBetween(Window.unboundedPreceding, Window.currentRow)

// COMMAND ----------

// 2. Determining a condition over this window of data
import org.apache.spark.sql.functions.max
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

// 3. Applying the conditions on the dataframe
dfWithDate.where("CustomerId is NOT NULL").orderBy("CustomerId")
.select(
col("CustomerId"),
  col("date"),
  col("Quantity"),
  maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()

// COMMAND ----------

// RollUp example
// get rid of null values
val dfNotNull = dfWithDate.drop()
dfNotNull.createOrReplaceTempView("dfNoNull")

// COMMAND ----------


// RollUp over 2 col data -> date and countries
// On which the aggregation of sum is then applied
// the row with null date and country is then the rollup value for this
val rollUpDF = dfNotNull.rollup("Date", "Country").agg(sum("Quantity").alias("totalQuantity")).selectExpr("Date", "Country", "totalQuantity").orderBy("Date").show(5)

// COMMAND ----------


