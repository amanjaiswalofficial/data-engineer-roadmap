from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, IntegerType, StructField, StructType
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("amountByCustomerOne").getOrCreate()

schema = StructType(
    [
        StructField("customerID", IntegerType(), True),
        StructField("orderID", IntegerType(), True),
        StructField("amount", FloatType(), True),
    ]
)

input = spark.read.schema(schema).csv(
    "E:/Projects/data-engineering-learning-path/07_pyspark_demos/customer-orders.csv"
)

total_amount_spent = (
    input.groupBy("customerID")
    .sum("amount")
    .withColumnRenamed("sum(amount)", "total_amount_spent")
    .sort("total_amount_spent")
)

total_amount_spent = total_amount_spent.withColumn(
    "total_amount_spent", func.round(func.col("total_amount_spent"), 2)
)

total_amount_spent.show()
