package com.yang.spark.day02.single

import org.apache.spark.{SparkConf, SparkContext}

object SortBy {
  def main(args: Array[String]): Unit = {
//1.得到SparkContext
val conf = new SparkConf().setMaster("local[2]").setAppName("SortBy")
val sc = new SparkContext(conf)
//2.创建RDD
val list = List(30,50,70,60,10,20)
val rdd = sc.parallelize(list,2)
//3.转化
val rdd2 = rdd.sortBy(x=>x,false)
//4.行动算子
rdd2.collect.foreach(println)
//5.关闭SparkContext
sc.stop()

  }
}
