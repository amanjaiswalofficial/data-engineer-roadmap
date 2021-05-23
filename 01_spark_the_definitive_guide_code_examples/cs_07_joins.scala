// Databricks notebook source
// 1st dataframe
val person = Seq(
(0, "Bill Chambers", 0, Seq(100)),
(1, "Matei Zaharia", 1, Seq(500, 250, 100)),
(2, "Michael Armbrust", 1, Seq(250, 100)))
.toDF("id", "name", "graduate_program", "spark_status")

// 2nd dataframe
val graduateProgram = Seq(
(0, "Masters", "School of Information", "UC Berkeley"),
(2, "Masters", "EECS", "UC Berkeley"),
(1, "Ph.D.", "EECS", "UC Berkeley"))
.toDF("id", "degree", "department", "school")

// 3rd dataframe
val sparkStatus = Seq(
(500, "Vice President"),
(250, "PMC Member"),
(100, "Contributor"))
.toDF("id", "status")


// COMMAND ----------

person.createOrReplaceTempView("person")
graduateProgram.createOrReplaceTempView("graduateProgram")
sparkStatus.createOrReplaceTempView("sparkStatus")

// COMMAND ----------

person.show()
graduateProgram.show()
sparkStatus.show()

// COMMAND ----------

val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
// Inner join 
person.join(graduateProgram, joinExpression).show()

// COMMAND ----------

// Outer Join, includes all the rows from both the tables, irrespective of whether the join was successful or not
person.join(graduateProgram, joinExpression, "outer").show()

// COMMAND ----------

// Left outer includes all rows from left table, and with the right table which match in the join
person.join(graduateProgram, joinExpression, "left_outer").show()

// COMMAND ----------

// Right outer includes all rows from right table, and with the left table which match in the join
person.join(graduateProgram, joinExpression, "right_outer").show()

// COMMAND ----------

// Left semi filters all the rows from the left table which have a match with right table
graduateProgram.join(person, joinExpression, "left_semi").show()

// COMMAND ----------

// Left semi filters all the rows from the left table which have a match with right table
person.join(graduateProgram, joinExpression, "left_semi").show()

// COMMAND ----------

// Left anti is opposite of left semi
// How to natural join?

// COMMAND ----------

// Join on complex types
import org.apache.spark.sql.functions.expr
val joinExpression = expr("array_contains(spark_status, id)")
person.withColumnRenamed("id", "personId").join(sparkStatus, joinExpression).show()

// COMMAND ----------


