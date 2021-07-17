from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


spark = SparkSession.builder.appName("leastPopularSuperHero").getOrCreate()


schema = StructType([StructField("ID",IntegerType(),True), StructField("Name",StringType(),True)])

marvel_names = (
    spark.read.option("sep", " ")
    .schema(schema)
    .csv("E:/Projects/data-engineering-learning-path/07_pyspark_demos/Marvel_Names.txt")
)

marvel_graph = spark.read.text(
    "E:/Projects/data-engineering-learning-path/07_pyspark_demos/Marvel_Graph.txt"
)

hero_and_count = marvel_graph.withColumn("ID", func.split(func.col("value"), " ")[0].cast(IntegerType())).withColumn("total_heroes", func.size(func.split(func.col("value"), " "))-1).drop("value")

hero_and_count_sorted = hero_and_count.groupBy("ID").agg(func.sum("total_heroes")).orderBy("sum(total_heroes)").withColumnRenamed("sum(total_heroes)", "total_hero_appearence")

minimum_appearence = hero_and_count_sorted.agg(func.min("total_hero_appearence")).first()[0]

least_appearence_heroes = hero_and_count_sorted.filter(func.col("total_hero_appearence")==minimum_appearence[0][0]).select("ID")

# df[col_name] useful when ambigious column
least_popular_heroes = least_appearence_heroes.join(marvel_names, least_appearence_heroes["ID"] == marvel_names["ID"]).select(least_appearence_heroes["ID"], "Name").collect()

for item in least_popular_heroes:
    hero_item = item.asDict()
    print(hero_item["ID"], hero_item["Name"])
