import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object firstObject {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("firstObjectDemo")
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")
    val words = sc.parallelize(myCollection, 2)

    println(words.distinct().count())



  }

}
