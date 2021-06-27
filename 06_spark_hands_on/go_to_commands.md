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
.option("maxFilesPerTrigger", 1)
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

Defining Custom schema while reading

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

Write to a format on disk

```scala
val PATH = ""
df.write.json(PATH)
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



### Dataframe SQL Methods

### Dataset API and related functions

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
df.na.replace("")
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

### RDD operations


### Basic ETL Example

### To Know More About

```
spark.conf.set("spark.sql.shuffle.partitions", "5")
// ROWS In Spark
// Using Regex with spark values
// Create custom dataframe
// Test na handling methods
// Create complex fields
// Applying functions on columns like UDF
```