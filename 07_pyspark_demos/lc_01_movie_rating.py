from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("sqlApp").getOrCreate()


mysql = (
    spark.read.format("jdbc")
    .option("url", "jdbc:mysql://localhost/bank")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("user", "root")
    .option("password", "root")
)

tables = ["test0", "test1", "test2"]
table_conn = list()

for table in tables:
    table_conn.append(mysql.option("dbtable", table).load())
