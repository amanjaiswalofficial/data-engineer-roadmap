// cs 03

// Reading a file and getting it schema based on in-built schema inference
spark.read.format("csv").load("./2015_summary.csv").schema

// COMMAND ----------

import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.functions.{col, column}

// Custom defining schema with a) name of col, b) type of column value c) whether it can be null and d) metadata if any
val myManualSchema = 
StructType(Array(
StructField("DEST_COUNTRY_NAME", StringType, true),
StructField("ORIGIN_COUNTRY_NAME", StringType, true),
StructField("count", LongType, false, Metadata.fromJson("{\"hello\": \"world\"}"))))

// Using same custom schema to read a file and attempt to use it over the file's data
val df = spark.read.format("csv").schema(myManualSchema).load("./2015_summary.csv")

// View columns from a file
spark.read.format("csv").schema(myManualSchema).load("./2015_summary.csv").columns

// COMMAND ----------

// Accessing a column by name
df.col("DEST_COUNTRY_NAME")

// COMMAND ----------

import org.apache.spark.sql.Row

// Create custom row and accessing its col data by index
val MyRow = Row("Hello", null, 1, false)
MyRow(0)
MyRow(3)
