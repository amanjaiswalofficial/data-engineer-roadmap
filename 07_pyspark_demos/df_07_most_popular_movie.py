from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType, StructField, StructType, TimestampType

spark = SparkSession.builder.appName("mostPopularMovie").getOrCreate()


input = spark.read.option("sep", "\t").csv(
    "E:/Projects/data-engineering-learning-path/07_pyspark_demos/ml-100k/u.data"
)
input = (
    input.withColumnRenamed("_c0", "userID")
    .withColumnRenamed("_c1", "movieID")
    .withColumnRenamed("_c2", "rating")
    .withColumnRenamed("_c3", "timestamp")
)
input = (
    input.withColumn("userID", func.col("userID").astype(IntegerType()))
    .withColumn("movieID", func.col("movieID").astype(IntegerType()))
    .withColumn("rating", func.col("rating").astype(IntegerType()))
    .withColumn("timestamp", func.col("timestamp").astype(TimestampType()))
)

most_popular_movies = (
    input.groupBy("movieID")
    .count()
    .sort(func.desc("count"))
    .withColumnRenamed("count", "most_popular")
    .collect()
)


for item in most_popular_movies:
    print(item.movieID, item.most_popular)
