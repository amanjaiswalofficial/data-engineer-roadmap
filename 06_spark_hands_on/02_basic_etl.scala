import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

val df = spark.read.option("header", "true").csv("<PATH_HERE>/supermarket_sales.csv")       

// EXTRACT
val df_grouped_by_city = df.groupBy("city").count()

// TRANSFORM
df_grouped_by_city.withColumnRenamed("count", "CityCount")

df_grouped_by_city.show()

// LOAD
df_grouped_by_city.write.json("<PATH_HERE>/supermarket_cities.json")