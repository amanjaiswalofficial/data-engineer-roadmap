### I/O

Reading normal CSV file

```scala
val PATH = ""
spark.read.format("csv").load(PATH)
```

Multiple file reading (JSONs)

```scala
val PATH = ""
val df = spark.read.option("recursiveFileLookup", "true").option("header", "true").json(PATH)
df.createOrReplaceTempView("artist_data") // Create a tempView to be used for SQL queries on the DF
```

Other Reading Options

```scala
.option("inferSchema", "true")
.schema(staticSchema)
.option("sep", ";")
.option("maxFilesPerTrigger", 1)
.coalesce(INTEGER NUMBER)
.option("sep", "\t")
.option("mode", "FAILFAST")
.option("multiLine", true) // especially to read multiline json Ex- not like {"": ""} in 1 line
```

Print/Display/Collect

```scala
df.take(n) // Print data in Array[Rows]
df.show(n) // Print data in form of table
df.show(n, false) // False tells to not hide data of any column and print complete val
df.[SOME_METHOD].limit(5)[.show()] // To limit no. of rows to be passed forward
df.printSchema() // Print schema of the loaded dataframe
df.columns // Print Array[] of schema of the dataframe
df.describe() // Basic stats about data
```

Writing to a tsv file

```scala
df.write.format("csv").mode("overwrite").option("sep", "\t").save("./tmp.tsv")
```

Write to a format on disk

```scala
val PATH = ""
df.write.json(PATH)
```

Enable option to ignore corrupt files while reading data from path/paths

```scala
spark.sql("set spark.sql.files.ignoreCorruptFiles=true")

// will ignore content of dir2 if it contains corrupted file and still make
// df out of content that isn't corrupt
val testCorruptDF = spark.read.parquet(
  "examples/src/main/resources/dir1/",
  "examples/src/main/resources/dir1/dir2/")

// similar option: spark.sql.files.ignoreMissingFiles
// also, read: recursive look up
```



### Common Dataframe related methods

Aggregation Examples / Others

```scala
df.groupBy("City").count() // Group by
df.groupBy("City").max() // Max entries of the value in the column city

// Others
df.sort("City")
```

Using functions

```scala
import org.apache.spark.sql.functions.max
df.select(max("City"))

import org.apache.spark.sql.functions.desc
df.groupBy("Rating").count().orderBy(desc("count"))

import org.apache.spark.sql.functions.asc
df.groupBy("Rating").count().orderBy(asc("count"))
```

Row type in spark

```scala
import org.apache.spark.sql.Row
val MyRow = Row("Hello", null, 1, false)
MyRow(0) // can be accessed via index
```

Seq type in spark

```scala
val simpleColors = Seq("black", "white", "red", "green", "blue")
simpleColors.take(5)
simpleColors.map(_.toUpperCase)
```

Aggregation on Dataframes

```scala
df.count() //  count no. of records

import org.apache.spark.sql.functions.countDistinct
df.select(countDistinct("City")) // distinct records for a particular column 

import org.apache.spark.sql.functions.{first, last, min, max, sum}
df.select(first("Rating").alias("First Rating"), last("Rating"), min("Rating"), max("Rating"))

df.groupBy("Customer type").count() // group by

// all 3 will return same response
import org.apache.spark.sql.functions._
df.groupBy("Rating").count()
df.groupBy("Rating").agg(count("Rating"))
df.groupBy("Rating").agg(expr("count(Rating)")).show(5)
```

Using col

```scala
import org.apache.spark.sql.functions.col
df.where(col("Rating")===9).limit(5) // === to compare equality, =!= for not equal to
df.where(col("Rating").geq(9)).limit(5) // greater than equal to
df.where(col("Rating")===9).select("City", "Rating").limit(5) // with select()
df.where(col("Rating")===9).select("City", "Rating").withColumnRenamed("City", "DataCity").limit(5) // rename column

// More functions like this
// ===, leq(), isin()
```

Using custom conditions

```scala
val conditionRatingMoreThanNine = col("Rating") > 9 // first condition
val cityIsNaypyitaw = col("City") === "Naypyitaw" // second condition
df.where(conditionRatingMoreThanNine.and(cityIsNaypyitaw)).limit(5) // .and() .or() etc
// Other similar functions to use to compare
// round, initcap, upper, lpad etc
```

Date related methods

```scala
import org.apache.spark.sql.functions.{current_date, current_timestamp}
val dateDF = spark.range(5)
.withColumn("today", current_date())
.withColumn("now", current_timestamp())

// subtract date
import org.apache.spark.sql.functions.{date_add, date_sub}
dateDF.select(date_sub($"today", 3)).show(5)

// similar methods
// date_add, datediff, month_between etc
```

Handling null values

```scala
df.na.drop()
df.na.drop("any") // ("all")
df.na.fill("")
df.na.replace("COL_NAME", ImmutableMap.of(previous_val, new_val))
```

UDF (Implementation with Spark SQL)

```scala
val squared = (s: Int) => {
  s * s
}
spark.udf.register("square", squared)
spark.range(1, 20).createOrReplaceTempView("tempData")
spark.sql("select square(id) from tempData").show()
```

Joins in dataframes

```scala
val person = Seq((0, "Aman Jaiswal", 1), (1, "Amit Kanderi", 1),  (2, "Arun Pratap", 2), (3, "Amit Dubey", 3)).toDF("id", "name", "subject_id")
val subjects = Seq((1, "Science"), (2, "Physics"), (3, "Chemistry"), (4, "Computer Science")).toDF("sub_id", "sub_name")
val classes = Seq((0, "X"), (1, "XI"), (2, "XII")).toDF("id", "class_name")

person.createOrReplaceTempView("personTable")
subjects.createOrReplaceTempView("subjectTable")
classes.createOrReplaceTempView("classTable")

val joinBetween = person("subject_id") === subjects("sub_id")
val joinBetween2 = person("id") === classes("id")
person.join(subjects, joinBetween, "left")
// IS SAME AS
spark.sql("select * from personTable join subjectTable on subjectTable.sub_id=personTable.subject_id")

person.join(classes, joinBetween2, "right")

// other joins
// left semi right semi left outer right outer
```

Using globalview

```scala
createGlobalView("") // something that persists over multiple spark sessions
```



### Dataframe SQL Methods

### Dataset API and related functions

Converting from dataframe to dataset (strict_type dataframes)

```scala
// 1st type
case class CustomSchema(COL_NAME_1: String, COL_NAME_2: Int, COL_NAME_3...)
val df = spark.read.format("csv").load(PATH)
val dfWithSchema = df.as[CustomSchema]

// 2nd type of schema
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
val myManualSchema = 
StructType(Array(
StructField("COL_NAME_1", StringType, true),
StructField("COL_NAME_2", StringType, true),
StructField("COL_NAME_3", LongType, false)))
val df = spark.read.format("csv").schema(myManualSchema)
```

Creating Data frame from file - Example 2

```scala
val df = spark.read.option("header", "true").option("inferSchema" , "true").csv("PATH/sample_data.csv")
case class SampleDataSchema(id: Int,first_name: String,last_name: String,email: String,gender: String,ip_address: String)
val sampleData = df.as[SampleDataSchema]
```

Aggregation functions and custom functions over dataset

```scala
def isEntryMale(data: SampleDataSchema): Boolean = {
  return data.gender == "Male"
  
}

sampleData.filter(dataItem => isEntryMale(dataItem))
sampleData.map(dataItem => (dataItem.id, dataItem.first_name))
sampleData.groupByKey(x => x.gender).count().show()
```

Join with dataset

```scala
case class GenderSchema(id: Int,full_name: String,code_name: String)
val genderDF = Seq((0, "Male", "M"), (1, "Female", "F"), (2, "Transgender", "T")).toDF("id", "full_name", "code_name")
val genderData = genderDF.as[GenderSchema]

val joinCondition = sampleData.col("gender") === genderData.col("full_name")
val joinResponse = sampleData.joinWith(genderDF, joinCondition).withColumnRenamed("_1", "dataCol").withColumnRenamed("_2", "genderCol")

//joinResponse.selectExpr("dataCol.id").show(5)
joinResponse.selectExpr("dataCol.id","genderCol.code_name").show(5)
```

Similarly Join with a data frame is also possible

```scala
val df2 = df.toDF()
df2.join(OTHER_DF, CONDITION, "join_type")
```





### RDD operations

Different ways to create RDD

```scala
val myRDD = sc.parallelize(Array("Monday", "Tuesday", "Wednesday", "Thursday", "Friday"), 2)

val myRDDByFile = spark.read.textFile("E:/Projects/data-engineering-learning-path/06_spark_hands_on/dummyFileForRdd.txt")

val myRDDFromAnotherRDD = myRDD.map(singleDay => (singleDay.charAt(0), singleDay))
```

Print content from RDD

```scala
myRDD.collect().foreach(println)
myRDD.show()
myRDD.take(5).foreach(println)
```

Convert RDD out of a dataset

```scala
val myRDDasRDD = myRDD.rdd
```

Read particular column of a RDD

```scala
val myRDDByFile = spark.read.textFile("E:/Projects/data-engineering-learning-path/06_spark_hands_on/sample_data.csv")

// using split to read a specific column (convert from DS to RDD if needed)
val firstNames = myRDDByFile.map(_.split(",")(1))
```

Similar operations on RDD

```scala
// basic word count example
val nameOccurenceCount = firstNames.map(name => (name, 1)).reduceByKey((x,y) => x+y).map(item => (item._2, item._1)).sortByKey(false)

val uniqueNames = nameOccurenceCount.map(item => (item._2))

uniqueNames.filter(item => item != "Seline") // only those items which arent seline
uniqueNames.filter(item => item == "Seline") // only those which are seline
```

Union/Actions in RDD

```scala
val rowHasSeline = myRDDByFile.filter(item => item.contains("Seline")).rdd
val rowHasZach = myRDDByFile.filter(item => item.contains("Zacherie")).rdd
rowHasZach.union(rowHasSeline).collect.foreach(println)
```

Extract header and remaining data from an RDD file

```scala
val myRDDCol = myRDDByFile.first()
val myRDDData = myRDDByFile.filter(item => !item.equals(myRDDCol))

// extract specific columns into a tuple
myRDDData.map(item => item.split(",")).map(item2 => (item2(1), item2(2)))

// extract specific columns into a tuple 2
myRDDData.map(item => item.split(",")).map(item2 => (item2(1), item2(2), item2(3)))
```

Transformations in RDD

```scala
import org.apache.spark.rdd.RDD
val rdd:RDD[String] = spark.read.textFile(PATH).rdd

val mapOnRdd = rdd.map(item => (item, item.length)) // (ABC, 3) (DEFG, 4)..

val mapPartitionOnRdd = rdd.mapPartitions(iterator => (iterator.map(item => (item, item.length))))

// map() vs mapByPartition() vs mapByPartitionIndex()

val rddDistinct  = rdd.distinct()

val data = sc.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)),3)

data.groupByKey()     // ('k', (5,6))...
data.reduceByKey(_+_) // ('k', 11)
data.reduceByKey(_*_) // ('k', 30)

val data = spark.sparkContext.parallelize(Seq(("maths",52), ("english",75), ("science",82), ("computer",65), ("maths",85)))

data.sortByKey()

// join(), union(), intersection()

rdd.repartition(NUM) // increase or decrease number of partitions
rdd.coalesce(NUM) // only decrease the number of partitions
```




### Basic ETL Example

### To Know More About

```
spark.conf.set("spark.sql.shuffle.partitions", "5")
// ROWS In Spark
// Using Regex with spark values
// Test na handling methods
// Create complex fields
// Applying functions on columns like UDF
// Using Window functions
// RDD vs DF vs DS
// Bucketing/Partitioning By
// Parallelize, usage?
// Checkpointing
// Using partitions while reading
// Writing with partitioning Based on a specific col
csvFile.limit(10).write.mode("overwrite").partitionBy("DEST_COUNTRY_NAME").save("/tmp/partitioned-files.parquet")
```

https://www.dbta.com/Editorial/Trends-and-Applications/Spark-and-the-Fine-Art-of-Caching-119305.aspx

https://spark.apache.org/docs/latest/sql-performance-tuning.html