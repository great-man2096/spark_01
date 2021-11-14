package com.yang.spark.day02.ddd

import org.apache.spark.{SparkConf, SparkContext}

object Double2 {
  def main(args: Array[String]): Unit = {
    //1.得到SparkContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("Double1")
    val sc = new SparkContext(conf)
    //2.创建RDD
    val list = List(30,50,70,60,10,20)
    val list2 = List(30,5,7,60,1,2)
    val rdd = sc.parallelize(list,2)
    val rdd2 = sc.parallelize(list2,2)
    //3.转化
    //val rdd3 = rdd.zip(rdd2)//拉链：对应的的分区和元素个数应该一样
    val rdd3 = rdd.zipPartitions(rdd2)((it1, it2) => {
      it1.zipAll(it2, 100, 200)
    })//拉链：分区数相等
    val rdd4 = rdd.zipWithIndex()
    //4.行动算子
    rdd3.collect.foreach(println)
    rdd4.collect.foreach(println)
    //5.关闭SparkContext
    sc.stop()

  }
}
