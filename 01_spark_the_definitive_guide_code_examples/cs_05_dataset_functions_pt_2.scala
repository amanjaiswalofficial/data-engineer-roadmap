// Databricks notebook source
import org.apache.spark.sql.functions.{current_date, current_timestamp}
val dateDF = spark.range(10)
.withColumn("today", current_date())
.withColumn("now", current_timestamp())

dateDF.createOrReplaceTempView("dateTable")
dateDF.show(2)
dateDF.printSchema()

// COMMAND ----------

import org.apache.spark.sql.functions.{date_add, date_sub}
dateDF.select(date_sub($"today", 5).alias("subbed_date")).show(5)

// COMMAND ----------

import org.apache.spark.sql.functions.{datediff, months_between, to_date}
dateDF.withColumn("week_ago", date_sub($"today", 7))
.select(datediff($"week_ago", $"today")).show(1)


// COMMAND ----------

import org.apache.spark.sql.functions.{to_date, lit}
val dateFormat = "yyyy-dd-MM"

val cleanDateDF = spark.range(5).select(
to_date(lit("2017-15-11"), dateFormat).alias("date")
)
cleanDateDF.createOrReplaceTempView("dateTable")
cleanDateDF.show(2)

// COMMAND ----------

// Working with nulls
// Coalesce takes 2 values, and returns 2nd value if 1st is none
// if not, returns 1st as expected

// drop rows containing null
// df.na.drop()
// df.na.drop("any")
// df.na.drop("all")

// Others include .fill(), .replace()

// COMMAND ----------

// Complex types in Spark
val df = spark.read.format("csv")
.option("header", "true")
.option("inferSchema", "true")
.load("/FileStore/tables/2010_12_01.csv")
df.createOrReplaceTempView("dfTable")


// COMMAND ----------

df.selectExpr("(Description, InvoiceNo) as complex").show(3, false)

import org.apache.spark.sql.functions.struct
val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))

complexDF.select("complex.Description")
complexDF.select($"complex".getField("Description"))

// COMMAND ----------

// Array operations
/*
Split
Array length
Contains
Explode
*/

// COMMAND ----------

// UDF Example - User Defined Functions
val udfExampleDF = spark.range(5).toDF("num")

// Defining UDF here 
def power3(number:Double):Double = number * number * number

// Dummy run
power3(2.0)

import org.apache.spark.sql.functions.udf

// Making UDF a usable function over dataframes
val power3udf = udf(power3(_:Double):Double)
udfExampleDF.select(power3udf($"num")).show()

// COMMAND ----------

// Using UDF or User Defined Functions in SQL

// 1. Register as sql function
spark.udf.register("power3", power3(_:Double):Double)

// 2. Use it inside as sql queries to get result
udfExampleDF.selectExpr("power3(num)").show(5)

// COMMAND ----------
