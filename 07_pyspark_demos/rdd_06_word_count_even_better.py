from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("wordCountEvenBetter").setMaster("local")
sc = SparkContext(conf=conf)

input = sc.textFile(
    "E:/Projects/data-engineering-learning-path/07_pyspark_demos/Book.txt"
)
words = input.flatMap(lambda x: x.split()).map(
    lambda word: (word, 1)
)  # (word1, 1), (word2, 1)
word_count_sum = words.reduceByKey(
    lambda item1, item2: item1 + item2
)  # (word1, 10), (word2, 50)
word_count_sum_sorted = (
    word_count_sum.map(lambda x: (x[1], x[0])).sortByKey().collect()
)  # (50, word1)

for count, item in word_count_sum_sorted:
    word = item.encode("ascii", "ignore")
    print(word.decode(), count)
