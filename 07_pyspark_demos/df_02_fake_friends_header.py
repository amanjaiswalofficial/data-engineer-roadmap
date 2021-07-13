from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName("FriendsNewExample").getOrCreate()


input = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(
        "E:/Projects/data-engineering-learning-path/07_pyspark_demos/fakefriends-header.csv"
    )
)


input.printSchema()

input.select("name").show()

input.filter(input.age < 20).show()

input.groupBy(input.age).count().show()

input.select(input.age, input.age + 10).show()

spark.stop()
