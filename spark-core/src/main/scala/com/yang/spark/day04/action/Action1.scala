package com.yang.spark.day04.action

import org.apache.spark.{SparkConf, SparkContext}

object Action1 {
  def main(args: Array[String]): Unit = {
  //1.得到SparkContext
  val conf = new SparkConf().setMaster("local[2]").setAppName("Action1")
  val sc = new SparkContext(conf)
  //2.创建RDD
  val list = List(30,50,70,60,10,20)
  val rdd = sc.parallelize(list,2)
  //3.转化
  /*val rdd2 = rdd.map(x => {
    println("map...")
    x
  }).filter(x => {
    println("filter")
    x > 20
  }).collect()*/
  val rdd2 = rdd.takeOrdered(3)(Ordering.Int.reverse)
    println(rdd2.toList)
  //5.关闭SparkContext
  sc.stop()

  }
}
//行动算子：collect，count，take，