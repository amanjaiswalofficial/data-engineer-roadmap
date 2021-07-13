from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("wordCountNew").getOrCreate()


input = spark.read.text(
    "E:/Projects/data-engineering-learning-path/07_pyspark_demos/Book.txt"
)


# converts a dataframe of string into rows of words
words = input.select(func.explode(func.split(input.value, "\\W+")).alias("word"))

# split on spaces, not based on regex
# input.select(func.explode(func.split(input.value, " ")).alias("Word")).show()

words = words.filter(words.word != "")

lower_case_words = words.select(func.lower(words.word).alias("word"))

lower_case_words = lower_case_words.groupBy("word").count()

lower_case_words.groupBy("word").count().sort("count")

total_count = lower_case_words.count()

print("Total words are", total_count)
