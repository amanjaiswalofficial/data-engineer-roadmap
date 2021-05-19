// Databricks notebook source
// Reading data from csv to display schema and create SQLView
val df = spark.read.format("csv")
.option("header", "true")
.option("inferSchema", "true")
.load("/FileStore/tables/2010_12_01.csv")
df.printSchema()
df.createOrReplaceTempView("dfTable")


// COMMAND ----------

import org.apache.spark.sql.functions.col

// show(3, false) is probably to whether hide content of column after certain no. of characters
df.where(col("InvoiceNo").equalTo(536365)).select("InvoiceNo", "Description").show(3, false)

// =!= for not equal to, === for equal to
df.where(col("InvoiceNo") === 536365).select("InvoiceNo", "Description").show(3)

// Another way for same condition
df.where("InvoiceNo = 536365").show(3, false)

// COMMAND ----------

// Using and & or in conditions for spark dataframe
val priceFilter = col("UnitPrice") > 600
val descFilter = col("Description").contains("POSTAGE")
df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descFilter)).show(2)

// COMMAND ----------

val DOTCodeFilter = col("StockCode") === "DOT"
val priceFilter = col("UnitPrice") > 600
val descFilter = col("Description").contains("POSTAGE")

// Using and condition 
df.withColumn("isExpensive", 
              DOTCodeFilter.and(priceFilter.or(descFilter)))
              .select("StockCode", "UnitPrice", "isExpensive").show(3, false)


// COMMAND ----------

import org.apache.spark.sql.functions.{expr, pow}

// using filters for mathematical operations
val fabricatedQuantity = pow(col("Quantity")* col("UnitPrice"), 2) + 5
df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)

// Similar response while using SQL query
df.selectExpr("CustomerId", "(POWER((Quantity * UnitPrice), 2)+5) as realQuantity").show(2)

// COMMAND ----------

import org.apache.spark.sql.functions.{round}

// Using round to round up values
df.select(round(col("UnitPrice"),1).alias("rounded"), col("UnitPrice")).show(3)

// COMMAND ----------

// Describe basic numerical stats about data
df.describe().show()

// COMMAND ----------

import org.apache.spark.sql.functions.monotonically_increasing_id

// Using to display id like numbers if required
df.select(monotonically_increasing_id().alias("id"), col("UnitPrice")).show(5)

// COMMAND ----------

// Basic String functions
/*
initcap
lower
upper
ltrim
rtrim
lpad
rpad
*/

// Using regex ops in spark
import org.apache.spark.sql.functions.regexp_replace
val simpleColors = Seq("black", "white", "red", "green", "blue")

val regexString = simpleColors.map(_.toUpperCase).mkString("|")
// regexString = "BLACK|WHITE|RED|GREEN|BLUE"

// regexp_replace works on col specified, applying regexString condition and replacing with "COLOR" word in the col
// every word out of BLACK WHITE RED GREEN BLUE is replaced by COLOR
df.select(regexp_replace(col("Description"), regexString, "COLOR")
          .alias("newColor"),col("Description")).show(5, false)

// COMMAND ----------

import org.apache.spark.sql.functions.translate

// Using translate to replace letters with something else, i.e. L with 1, E with 3 and so on
df.where(col("Description").contains("WHITE")).select(translate(col("Description"), "LEET", "1337"), col("Description")).show(5)
//df.where(col("Description").contains("LEET")).show(4)

// COMMAND ----------

import org.apache.spark.sql.functions.regexp_extract
val regexString = simpleColors.map(_.toUpperCase).mkString("(","|", ")")
// (BLACK|WHITE|RED|GREEN|BLUE)

// regexp extract example
df.select(regexp_extract(col("Description"), regexString, 1).alias("color_clean"), 
          col("Description")).show(4, false)

// COMMAND ----------


