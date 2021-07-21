from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[*]").appName("EnglandCouncilsJob").getOrCreate())


data = spark.sparkContext.textFile("E:/Projects/data-engineering-learning-path/07_pyspark_demos/data/england_councils/district_counsils.csv")
spark.read.format("csv").option("header", "true").load("E:\\Projects\\data-engineering-learning-path\\07_pyspark_demos\data\\england_councils\\district_councils.csv")


from pyspark.sql.functions import input_file_name
from pyspark.sql import functions as func

df = spark.read.format("csv").option("header", "true").load("E:\\Projects\\data-engineering-learning-path\\07_pyspark_demos\data\\england_councils\\*.csv").withColumn("file_name", input_file_name())
df2 = df.withColumn("file_name", func.split(func.col("file_name"), "/"))
df3 = df2.withColumn("council_type", func.reverse("file_name")[0])
council_name_mapper = {"district_councils.csv": "District", "london_boroughs.csv": "London", "metropolitan_districts.csv": "Metropolitan", "unitary_authorities.csv": "Unitary"}
council_names = spark.sparkContext.broadcast(council_name_mapper)

def get_value(input_key):
    return council_names.value[input_key]



