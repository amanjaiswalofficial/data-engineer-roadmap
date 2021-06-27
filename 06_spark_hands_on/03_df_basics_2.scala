// UDF example
import org.apache.spark.sql.functions.{udf, lit}
case class subRecord(x: Int)
case class ArrayElement(foo: String, bar: Int, values: Array[Double])
case class Record(an_arr: Array[Int], 
                  a_map: Map[String, String], 
                  a_struct: subRecord, 
                  an_array_of_struct: Array[ArrayElement])


val df = sc.parallelize(Seq(
  Record(Array(1, 2, 3), Map("foo" -> "bar"), subRecord(1),
         Array(
           ArrayElement("foo", 1, Array(1.0, 2.0, 2.0)),
           ArrayElement("bar", 2, Array(3.0, 4.0, 5.0)))),
  Record(Array(4, 5, 6), Map("foz" -> "baz"), subRecord(2),
         Array(ArrayElement("foz", 3, Array(5.0, 6.0)), 
               ArrayElement("baz", 4, Array(7.0, 8.0))))
)).toDF
df.createOrReplaceTempView("myTempData")
df.show(false)