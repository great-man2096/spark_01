package com.yang.spark.day02.single

import org.apache.spark.{SparkConf, SparkContext}

object GroupBy {
  def main(args: Array[String]): Unit = {
    //1.得到SparkContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("GroupBy")
    val sc = new SparkContext(conf)
    //2.创建RDD
    val array = Array(30,5,7,60,1,20)
    //val rdd = sc.parallelize(array)
    val rdd = sc.parallelize(array,2)
    //3.转化
    val rdd2 = rdd.groupBy(x => x % 2)
    val rdd3 = rdd2.map {
      case (k, it) => (k, it.sum)
    }
    //4.行动算子
    rdd3.collect.foreach(println)
    //5.关闭SparkContext
    sc.stop()

  }
}
