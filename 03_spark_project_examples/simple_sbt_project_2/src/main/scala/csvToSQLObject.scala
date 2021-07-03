import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object csvToSQLObject {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("csvToSQLDemo")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

//    How to connect to MYSQL via Spark
//    val studentSQLDF = spark.read.format("jdbc").
//      option("url", "jdbc:mysql://localhost:3306/northwind").
//      option("driver", "com.mysql.jdbc.Driver")
//      .option("dbtable", "testingTable")
//      .option("user", "root")
//      .option("password", "root")
//      .load()

    val studentCSVDF = spark.read.format("csv").option("header", "true")
      .load("PATH_HERE/student_data.csv")

    // not needed when header is true
    val renamedDF = studentCSVDF
      .withColumnRenamed("_c0", "name")
      .withColumnRenamed("_c1", "class")
      .withColumnRenamed("_c2", "roll")
      .withColumnRenamed("_c3", "marks")
    renamedDF.write.format("jdbc").
      option("url", "jdbc:mysql://localhost:3306/northwind").
      option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "testingTable")
      .option("user", "root")
      .option("password", "root").mode("append").save()

  }
}
