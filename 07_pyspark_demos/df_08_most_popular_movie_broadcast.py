from pyspark.sql import SparkSession, functions
import codecs

from pyspark.sql.types import IntegerType, LongType, StructField, StructType

spark = SparkSession.builder.appName("moviePopularTwo").getOrCreate()


def load_values():
    movie_ratings = dict()
    with codecs.open(
        "E:/Projects/data-engineering-learning-path/07_pyspark_demos/ml-100k/u.item",
        mode="r",
        encoding="ISO-8859-1",
        errors="ignore",
    ) as file:
        for line in file:
            fields = line.split("|")
            movie_ratings[int(fields[0])] = fields[1]

    return movie_ratings


loaded_dict = spark.sparkContext.broadcast(load_values())

schema = StructType(
    [
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

input = (
    spark.read.schema(schema)
    .option("sep", "\t")
    .csv("E:/Projects/data-engineering-learning-path/07_pyspark_demos/ml-100k/u.data")
)

movie_counter = input.groupBy("movieID").count().sort(functions.desc("count"))


def get_movie_name(movie_id):
    return loaded_dict.value[movie_id]


get_movie_name_udf = functions.udf(get_movie_name)

movie_counter.withColumn(
    "movie_name", get_movie_name_udf(functions.col("movieID"))
).limit(1).select("movie_name").show()
