package day01

import org.apache.spark.sql.SparkSession

object CreatDS {
  def main(args: Array[String]): Unit = {
        val spark = SparkSession
              .builder()
              .appName("CreatDS").master("local[2]")
              .getOrCreate()
            import spark.implicits._
    val ints = List(3, 4, 45326, 245, 21)
    val ds = ints.toDS()
    ds.show
            spark.close()
  }
}
