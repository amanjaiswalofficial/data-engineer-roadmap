import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}

val myManualSchema = new StructType(Array(
    new StructField("DEST_COUNTRY_NAME", StringType, true),
    new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
    new StructField("count", LongType, false)
))

// reading csv with schema
spark.read.format("csv")
.option("header", "true")
.option("mode", "FAILFAST")
.schema(myManualSchema)
.load("./tables/flight-data/csv/2010-summary.csv")
.show(5)

// COMMAND ----------

val wrongSchema = new StructType(
    Array(
        new StructField("DEST_COUNTRY_NAME", LongType, true),
        new StructField("ORIGIN_COUNTRY_NAME", LongType, true),
        new StructField("count", LongType, false)
    )
)

// trying to read csv with incorrect or not matching schema
spark.read.format("csv")
.option("header", "true")
.option("mode", "FAILFAST")
.schema(wrongSchema)
.load("./tables/flight-data/csv/2010-summary.csv")
.show(5)

// COMMAND ----------

val csvFile = spark.read.format("csv")
.option("header", "true")
.option("mode", "FAILFAST")
.schema(myManualSchema)
.load("./tables/flight-data/csv/2010-summary.csv")

// writing to a tsv file
csvFile.write.format("csv").mode("overwrite").option("sep", "\t").save("./tmp.tsv")

// COMMAND ----------

// reading a json file
spark.read.format("json").option("mode", "FAILFAST").schema(myManualSchema)
.load("./tables/flight-data/json/2010-summary.json").show(5)

val csvFile = spark.read.format("csv")
.option("header", "true")
.option("mode", "FAILFAST")
.schema(myManualSchema)
.load("./tables/flight-data/csv/2010-summary.csv")

// writing a json file
csvFile.write.format("json").mode("overwrite").save("./tmp.json")


// COMMAND ----------

// reading a parquet file
spark.read.format("parquet").load("./tables/flight-data/parquet/2010-summary.parquet").show(5)

val csvFile = spark.read.format("csv")
.option("header", "true")
.option("mode", "FAILFAST")
.schema(myManualSchema)
.load("./tables/flight-data/csv/2010-summary.csv")

// writing a parquet file
csvFile.write.format("parquet").mode("overwrite")
.save("/tmp/my-parquet-file.parquet")

// COMMAND ----------

// require to copy sqlite jdbc jar to spark/jars
val driver = "org.sqlite.JDBC"
val path = "[db_path]/my-sqlite.db"
val url = s"jdbc:sqlite:/${path}"
val tablename = "flight_info"

// reading from sqlite source
val dbDataFrame = spark.read.format("jdbc")
.option("url", url)
.option("dbtable", tablename)
.option("driver", driver)
.option("mode", "permissive").load()

print(dbDataFrame.show(2))

// COMMAND -----------


// More
// Reading from databases
// Writing to databases
// Using partitions while reading
/*
Writing with partitioning
Based on a specific col

csvFile.limit(10).write.mode("overwrite").partitionBy("DEST_COUNTRY_NAME")
.save("/tmp/partitioned-files.parquet")
*/

/*
Bucketing
*/

