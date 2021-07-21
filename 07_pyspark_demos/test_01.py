from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, FloatType


class CouncilJob:
    def __init__(self):
        self.spark_session = (
            SparkSession.builder.master("local[*]").appName("councilJob").getOrCreate()
        )

    def extract_council(self):
        from pyspark.sql.functions import input_file_name
        spark = self.spark_session
        schema = StructType([StructField("council", StringType(), True),StructField("county", StringType(), True),StructField("file_name", StringType(), True)])
        df = (spark.read.format("csv").schema(schema).option("header", "true").csv("E:/Projects/data-engineering-learning-path/07_pyspark_demos/data/england_councils/*.csv").withColumn("file_name", input_file_name()))
        df2 = df.withColumn("file_name_split", func.split(func.col("file_name"), "/")).drop("file_name")
        df3 = df2.withColumn("file_name", func.reverse(func.col("file_name_split"))[0]).drop("file_name_split")
        name_mapper = {'london_boroughs.csv': 'London', 'district_councils.csv': 'District', 'unitary_authorities.csv': 'Unitary', 'metropolitan_districts.csv': 'Metropolitan'}
        broadcasted_name = spark.sparkContext.broadcast(name_mapper)
        df4 = df3.rdd.map(lambda x: (x[0], x[1], broadcasted_name.value[x[2]])).toDF(schema)
        df5 = df4.withColumnRenamed("file_name", "council_type")
        return df5

    def extract_avg_price(self):
        
        spark = self.spark_session

        df_avg_price = spark.read.format("csv").option("header", "true").csv("E:/Projects/data-engineering-learning-path/07_pyspark_demos/data/property_avg_price.csv")
        df_avg_price = df_avg_price.withColumn("avg_price_nov_2019", func.col("avg_price_nov_2019").cast(FloatType())).withColumn("avg_price_nov_2018", func.col("avg_price_nov_2018").cast(FloatType()))
        df = df_avg_price.withColumnRenamed("local_authority", "council").select("council", "avg_price_nov_2019")
        return df

    def extract_sales_volume(self):
        spark = self.spark_session
        schema = StructType([StructField("local_authority", StringType(), True), StructField("sales_volume_sep_2019", IntegerType(), True), StructField("sales_volume_sep_2018", IntegerType(), True)])
        df_sales_volume = spark.read.format("csv").schema(schema).option("header", "true").csv("E:/Projects/data-engineering-learning-path/07_pyspark_demos/data/property_sales_volume.csv")
        df = df_sales_volume.withColumnRenamed("local_authority", "council").select("council", "sales_volume_sep_2019")
        return df

    def transform(self, df_city_council: DataFrame, df_avg_price: DataFrame, df_sales_volume: DataFrame):
        spark = self.spark_session
        first_join = df_avg_price.join(df_sales_volume, df_avg_price["council"].eqNullSafe(df_sales_volume["council"]), "full_outer").select(df_avg_price["council"], "avg_price_nov_2019", "sales_volume_sep_2019")
        df = df_city_council.join(first_join, df_city_council["council"].eqNullSafe(first_join["council"]), "left").select(df_city_council["council"], "county", "council_type", "avg_price_nov_2019", "sales_volume_sep_2019")
        return df

c_job = CouncilJob()
df_1 = c_job.extract_council()
df_2 = c_job.extract_avg_price()
df_3 = c_job.extract_sales_volume()
final_df = c_job.transform(df_1, df_2, df_3)
final_df.show()





