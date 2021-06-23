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

