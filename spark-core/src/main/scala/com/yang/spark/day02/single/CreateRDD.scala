package com.yang.spark.day02.single

import org.apache.spark.{SparkConf, SparkContext}

object CreateRDD {
  def main(args: Array[String]): Unit = {
    //1.得到SparkContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("CraeteRDD")
    val sc = new SparkContext(conf)
    //2.创建RDD
    val array = Array(1, 3, 5, 8, 6, 7)
    //val rdd = sc.parallelize(array)
    val rdd = sc.makeRDD(array)
    //3.转换
    val rdd2 = rdd.map(_ * 2)
    //4.行动算子
    val array1 = rdd2.collect()
    array1.foreach(println)
    //5.关闭SparkContext
    sc.stop()
  }
}
