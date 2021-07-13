from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(",")
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)


lines = sc.textFile(
    "E:/Projects/data-engineering-learning-path/07_pyspark_demos/fakefriends.csv"
)
rdd = lines.map(parse_line)

totals_by_age = rdd.mapValues(lambda value: (value, 1)).reduceByKey(
    lambda value1, value2: (value1[0] + value2[0], value1[1] + value2[1])
)
average_by_age = totals_by_age.mapValues(lambda value: value[0] / value[1])
results = average_by_age.collect()

for age, average in results:
    print(age, average)
