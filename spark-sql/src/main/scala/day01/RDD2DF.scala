package day01

import org.apache.spark.sql.SparkSession

object RDD2DF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("RDD2DF").master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val rdd = sc.parallelize(("lisi", 10) :: ("zs", 11) :: Nil)
   // rdd.toDF("name","age").show
    val rdd2 = sc.parallelize(Array(User("Lisi", 10), User("zs", 20)))
    rdd2.toDF("1","2").show
    spark.close()
  }

  case class User(name: String, age: Int)

}