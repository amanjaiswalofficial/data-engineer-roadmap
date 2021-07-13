from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("FriendsByAgeNew").getOrCreate()

input = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(
        "E:/Projects/data-engineering-learning-path/07_pyspark_demos/fakefriends-header.csv"
    )
)

# avg number of friends for each age
input.groupBy("age").avg("friends").show()

# sorted answer
input.groupBy("age").avg("friends").sort("age").show()

# using func and alias
input.groupBy("age").agg(func.avg("friends").alias("average_friends")).show()

input.groupBy("age").agg(
    func.round(func.avg("friends"), 2).alias("average_friends")
).show()


spark.stop()
