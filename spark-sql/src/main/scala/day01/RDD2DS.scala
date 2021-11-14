package day01

import day01.RDD2DF.User
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object RDD2DS {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
          .builder()
          .appName("RDD2DS").master("local[2]")
          .getOrCreate()
        import spark.implicits._
    val sc = SparkContext.getOrCreate()
    val rdd = sc.parallelize(Array(User("a", 1), User("b", 2)))
    val ds = rdd.toDS()
    val rdd1 = ds.rdd
rdd1.foreach(println)
        spark.close()
  }
}
