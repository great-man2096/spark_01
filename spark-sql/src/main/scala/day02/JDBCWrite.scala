package day02

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

object JDBCWrite {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
          .builder()
          .appName("JDBCWrite").master("local[2]")
          .getOrCreate()
        import spark.implicits._
    val df = spark.read.json("C:\\tmp\\新建文本文档.txt")
    df.show()
    /*df.write
      .option("url", "jdbc:mysql://hadoop002:3306/company")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "staff3")
      .mode("overwrite")
      .format("jdbc").save()*/
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "123456")
    df.write.mode(SaveMode.Append).jdbc("jdbc:mysql://hadoop002:3306/company","staff3",properties)
        spark.close()
  }
}
