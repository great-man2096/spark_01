package com.yang.spark.day04.action

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Persit {
  def main(args: Array[String]): Unit = {
    //1.得到SparkContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("Persit")
    val sc = new SparkContext(conf)
    //2.创建RDD
    val list = List(30, 50, 70, 60, 10, 20)
    val rdd = sc.parallelize(list, 2)
    //3.转化
    val rdd2 = rdd.map(x => {
      println("map:" + x)
      x
    })
    val rdd3 = rdd2.filter(x => {
      println("filter" + x)
      true
    })
    //rdd3.persist(StorageLevel.MEMORY_ONLY)//做了持久化的计划，当第一个行动算子执行完毕之后，就会对这个RDD做持久化
    rdd3.cache()
    //4.行动算子
    rdd3.collect
    println("==========")
    rdd3.collect
    println("==========")
    rdd3.collect
    println("==========")
    //5.关闭SparkContext
    sc.stop()

  }
}
