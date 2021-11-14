package com.yang.spark.day05

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object DBCRead {
  def main(args: Array[String]): Unit = {
  //1.得到SparkContext
  val conf = new SparkConf().setMaster("local[2]").setAppName("DBCRead")
  val sc = new SparkContext(conf)
    val url = "jdbc:mysql://hadoop002:3306/company"
  //2.创建RDD
  val rdd = new JdbcRDD[String](
    sc,
    () => {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection(url, "root", "123456")
  },
    "select * from staff where ? <= id and id <= ?",
    1,
    10,
    2,
    (resultSet: ResultSet) => resultSet.getString(2)
  )

  //3.转化
  
  //4.行动算子
  rdd.collect.foreach(println)
  //5.关闭SparkContext
  sc.stop()
  
  }
}
