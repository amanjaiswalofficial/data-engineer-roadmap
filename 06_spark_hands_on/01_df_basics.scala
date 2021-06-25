import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

val df = spark.read.option("recursiveFileLookup", "true").option("header", "true").json("E:/lastfm_subset")
df.createOrReplaceTempView("artist_data")

var sqlArtists = spark.sql("select artist, count(*) from artist_data group by artist order by count(*) desc")
sqlArtists.show(5)

var dfArtists = df.groupBy("artist").count().withColumnRenamed("count", "Song Count").orderBy(desc("count"))
dfArtists.show(5)


sqlArtists = spark.sql("select artist, count(*) from artist_data group by artist order by count(*) desc limit 1")
sqlArtists.show(1)

dfArtists =  df.groupBy("artist").count().orderBy(desc("count")).select($"artist", $"count")
dfArtists.show(1)


// DAY 2
spark.read.format("csv").load("E:/../sample_data.csv").schema

val manualSchema = StructType(Array(
    StructField("id", StringType, true),
    StructField("first_name", StringType, true),
    StructField("last_name", StringType, true),
    StructField("email", StringType, true),
    StructField("gender", StringType, true),
    StructField("ip_adress", StringType, true)
))

 val df = spark.read.format("csv")
 .option("header", true)
 .schema(manualSchema)
 .load("E:/../sample_data.csv")


val df2 = df.select("id", "first_name")

val df2 = df.map(item => (item.getString(0), item.getString(1)))


