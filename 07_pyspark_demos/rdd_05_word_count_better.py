from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("wordCountBetter")
sc = SparkContext(conf=conf)

input = sc.textFile(
    "E:/Projects/data-engineering-learning-path/07_pyspark_demos/Book.txt"
)
words = input.flatMap(lambda x: re.compile(r"\W+", re.UNICODE).split(x))
word_count = words.countByValue()

for item, count in dict(
    sorted(dict(word_count).items(), key=lambda x: x[1], reverse=True)
).items():
    print(item, count)
