package day02

import java.util.Properties

import org.apache.spark.sql.SparkSession

object JDBCRead {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
          .builder()
          .appName("JDBCRead").master("local[2]")
          .getOrCreate()
        import spark.implicits._
   /* val df = spark.read
      .option("url", "jdbc:mysql://hadoop002:3306/company")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "staff")
      .format("jdbc").load()*/
   val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "123456")
    val df = spark.read.jdbc("jdbc:mysql://hadoop002:3306/company", "staff", properties)
    df.show
        spark.close()
  }
}
