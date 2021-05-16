// cs refers to cluster snippets

// Reading file from CSV path
val flightData2015 = spark.read.option("inferSchema", "true").option("header", "true").csv("./2015_summary.csv")

// Print first 3 elements
flightData2015.take(3)

// Sorting data after setting partitions, 
// Shuffle Partition is inversely proportional to time taken to sort
spark.conf.set("spark.sql.shuffle.partitions", "5")
flightData2015.sort("count").take(2)


// Create sql view from a csv loaded data
flightData2015.createOrReplaceTempView("flight_data_2015")


// Explaining strategy taken by spark for a sql operation and a set of dataframe operations
// In SQL
val sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""")
sqlWay.explain

// Using Dataframe
val dataFrameWay = flightData2015
.groupBy('DEST_COUNTRY_NAME)
.count()
dataFrameWay.explain


// Query to get max count from data
// In SQL
spark.sql("select max(count) from flight_data_2015").take(1)

// Using dataframe
import org.apache.spark.sql.functions.max
flightData2015.select(max("count")).take(1)


// Listing top 5 countries based on sum of count
// In SQL
val maxSql = 
spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as dest_total 
FROM flight_data_2015 
GROUP BY DEST_COUNTRY_NAME 
ORDER BY sum(count) DESC 
LIMIT 5""")
maxSql.show()

// Using Dataframe
import org.apache.spark.sql.functions.desc
flightData2015.groupBy("DEST_COUNTRY_NAME").sum("count").sort(desc("sum(count)")).limit(5).show()