package com.yang.spark.day03.kv

import org.apache.spark.{SparkConf, SparkContext}

object Cogroup {
  def main(args: Array[String]): Unit = {
    //1.得到SparkContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("Cogroup")
    val sc = new SparkContext(conf)
    //2.创建RDD
    val list = List((1,30),(2,50),(1,70))
    val list1 = List((1,"a"),(2,"b"),(1,"aa"))
    val rdd = sc.parallelize(list,2)
    val rdd1 = sc.parallelize(list1,2)
    //3.转化
    val rdd2 = rdd.cogroup(rdd1)
    //4.行动算子
rdd2.collect.foreach(println)
    //5.关闭SparkContext
    sc.stop()

  }
}
