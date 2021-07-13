from pyspark.sql import SparkSession
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("minTemperatureOne").getOrCreate()


schema = StructType(
    [
        StructField("stationID", StringType(), True),
        StructField("date", IntegerType(), True),
        StructField("measure_type", StringType(), True),
        StructField("temperature", FloatType(), True),
    ]
)

input = spark.read.schema(schema).csv(
    "E:/Projects/data-engineering-learning-path/07_pyspark_demos/1800.csv"
)
input.printSchema()


input.count()
min_temperatures = input.filter(input.measure_type == "TMIN")
min_temperatures = (
    min_temperatures.select("stationID", "temperature")
    .groupBy("stationID")
    .min("temperature")
    .withColumnRenamed("min(temperature)", "min_temperature")
)

min_temperatures = min_temperatures.withColumn(
    "min_temperature", func.round(func.col("min_temperature") * 0.1 * (9 / 5) + 32, 2)
)


min_temperatures.show()
