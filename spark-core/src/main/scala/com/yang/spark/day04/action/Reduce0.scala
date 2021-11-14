package com.yang.spark.day04.action

import org.apache.spark.{SparkConf, SparkContext}

object Reduce0 {
  def main(args: Array[String]): Unit = {
  //1.得到SparkContext
  val conf = new SparkConf().setMaster("local[2]").setAppName("Reduce0")
  val sc = new SparkContext(conf)
  //2.创建RDD
  val list = List(30,50,70,60,10,20)
  val rdd = sc.parallelize(list,2)
  //3.转化
    println(rdd.reduce(_ + _))

  //5.关闭SparkContext
  sc.stop()

  }
}
