package com.yang.spark.day05

import org.apache.spark.{SparkConf, SparkContext}

object TestFile {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","yang")
  //1.得到SparkContext
  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("TestFile")
  val sc = new SparkContext(conf)
  //2.创建RDD
  val list = "a"::"a b c"::Nil
  val rdd = sc.parallelize(list,2)
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .saveAsTextFile("/word2222")
  //3.转化

  //4.行动算子

  //5.关闭SparkContext
  sc.stop()

  }
}
