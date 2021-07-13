from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("friendsByAgeNew").getOrCreate()


def mapper(line):
    fields = line.split(",")
    return Row(
        ID=int(fields[0]),
        name=str(fields[1]),
        age=int(fields[2]),
        numFriends=int(fields[3]),
    )


lines = spark.sparkContext.textFile(
    "E:/Projects/data-engineering-learning-path/07_pyspark_demos/fakefriends.csv"
)
people = lines.map(mapper)

schema_people = spark.createDataFrame(people)
schema_people.createOrReplaceTempView("people")

teenagers = spark.sql("SELECT * from people where age >= 13 and age <= 19")

for teenager in teenagers.collect():
    print(teenager)


# can also do it this way
# schema_people.groupBy("age").count().orderBy("age")
