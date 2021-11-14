package com.yang.spark.day02.ddd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Double1 {
  def main(args: Array[String]): Unit = {
    //1.得到SparkContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("Double1")
    val sc = new SparkContext(conf)
    //2.创建RDD
    val list = List(30,50,70,60,10,20)
    val list2 = List(30,5,7,6,1,2)
    val rdd = sc.parallelize(list,2)
    val rdd2 = sc.parallelize(list2,3)
    //3.转化
    //val rdd3: RDD[Int]=rdd.union(rdd2)
    //val rdd3 = rdd.intersection(rdd2)
    //val rdd3 = rdd.subtract(rdd2)
    val rdd3 = rdd.cartesian(rdd2)//笛卡尔积
    //4.行动算子
    rdd3.collect.foreach(println)
    //5.关闭SparkContext
    sc.stop()

  }
}
