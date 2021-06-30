import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object secondObject {
  def main(args: Array[String]) {

    val logFile = "<PATH_HERE>/dummy.md"

    val conf = new SparkConf().setMaster("local[*]").setAppName("firstObjectDemo")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val logData = spark.read.textFile(logFile).cache()

    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}
