from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql import functions

spark = SparkSession.builder.appName("mostPopularSuperHero").getOrCreate()

schema = StructType(
    [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
)

marvel_names = (
    spark.read.option("sep", " ")
    .schema(schema)
    .csv("E:/Projects/data-engineering-learning-path/07_pyspark_demos/Marvel_Names.txt")
)

marvel_graph = spark.read.text(
    "E:/Projects/data-engineering-learning-path/07_pyspark_demos/Marvel_Graph.txt"
)

marvel_graph_with_num_connections = (
    marvel_graph.withColumn("id", functions.split(functions.col("value"), " ")[0])
    .withColumn(
        "connections", functions.size(functions.split(functions.col("value"), " ")) - 1
    )
    .drop("value")
)
marvel_graph_with_num_connections = marvel_graph_with_num_connections.groupBy("id").agg(
    functions.sum("connections").alias("total_conn")
)

most_popular_hero = marvel_graph_with_num_connections.sort(
    functions.col("total_conn").desc()
).first()

most_popular_hero_name = (
    marvel_names.filter(functions.col("id") == most_popular_hero.id)
    .select("name")
    .first()
)

print(most_popular_hero[0], most_popular_hero_name[0])
