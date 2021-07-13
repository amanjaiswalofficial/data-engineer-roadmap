from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCountExampleOne")
sc = SparkContext(conf=conf)

input = sc.textFile(
    "E:/Projects/data-engineering-learning-path/07_pyspark_demos/Book.txt"
)
words = input.flatMap(lambda line: line.split())
# word_counts = words.map(lambda item: (item,1)).reduceByKey(lambda x,y : x+y)
word_counts = words.countByValue()
for word, count in word_counts.items():
    print(word, count)
