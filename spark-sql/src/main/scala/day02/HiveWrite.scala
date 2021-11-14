package day02

import org.apache.spark.sql.{SaveMode, SparkSession}

object HiveWrite {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "yang")
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("HiveWrite").master("local[2]")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop001:9000/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._
    /*spark.sql("create database yang2")
    //spark.sql("show databases").show
    spark.sql("use yang2")
    spark.sql("create table user1(id int,name string)").show()
    spark.sql("insert into user1 values(10,'lisi')")*/
    spark.sql("use yang2")
    val df = spark.read.json("C:\\tmp\\新建文本文档.txt")
   // df.write.mode(SaveMode.Append).saveAsTable("user2")
    df.coalesce(1)
    df.write.mode(SaveMode.Append).insertInto("user2")
    spark.close()
  }
}
