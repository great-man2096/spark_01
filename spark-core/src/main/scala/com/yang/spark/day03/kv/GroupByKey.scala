package com.yang.spark.day03.kv

import org.apache.spark.{SparkConf, SparkContext}

object GroupByKey {
  def main(args: Array[String]): Unit = {
//1.得到SparkContext
val conf = new SparkConf().setMaster("local[2]").setAppName("GroupByKey")
val sc = new SparkContext(conf)
//2.创建RDD
val rdd = sc.parallelize(Array("a", "a", "b", "d", "y", "a", "d"), 2)
//3.转化
val wordOne = rdd.map((_, 1))
    val rdd2 = wordOne.groupByKey().mapValues(_.sum)
//4.行动算子
rdd2.collect.foreach(println)
//5.关闭SparkContext
sc.stop()

  }
}
