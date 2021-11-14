package com.yang.spark.day04.action

import org.apache.spark.{SparkConf, SparkContext}

object CountByKey {
  def main(args: Array[String]): Unit = {
    //1.得到SparkContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("ReduceByKey")
    val sc = new SparkContext(conf)
    //2.创建RDD
    val rdd = sc.parallelize(Array("a", "a", "b", "d", "y", "a", "d"), 2)
    println(rdd.map((_, 1)).countByKey())

  }
}
