package day02

import org.apache.spark.sql.SparkSession

object HiveRead {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
          .builder()
          .enableHiveSupport()
          .appName("HiveRead").master("local[2]")
          .getOrCreate()
        import spark.implicits._
    //
        spark.sql("show databases")
        spark.sql("use default")
        spark.sql("show tables").show

        spark.close()
  }
}
