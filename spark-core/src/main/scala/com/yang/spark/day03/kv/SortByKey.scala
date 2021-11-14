package com.yang.spark.day03.kv

import org.apache.spark.{SparkConf, SparkContext}

object SortByKey {
  def main(args: Array[String]): Unit = {
    //1.得到SparkContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("ReduceByKey")
    val sc = new SparkContext(conf)
    //2.创建RDD
    val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("b", 4), ("d", 3), ("y", 6), ("a", 8), ("d", 3)), 3)
    //3.转化
    val rdd2 = rdd.sortBy(_._2,false)
    //4.行动算子
    rdd2.collect.foreach(println)

    //5.关闭SparkContext
    sc.stop()
  }
}
