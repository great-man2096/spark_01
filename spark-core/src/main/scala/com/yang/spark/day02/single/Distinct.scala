package com.yang.spark.day02.single

import org.apache.spark.{SparkConf, SparkContext}

object Distinct {
  def main(args: Array[String]): Unit = {
//1.得到SparkContext
val conf = new SparkConf().setMaster("local[2]").setAppName("Distinct")
val sc = new SparkContext(conf)
//2.创建RDD
val list = List(30,50,70,60,10,20,10,70)
val rdd = sc.parallelize(list,2)
//3.转化
val rdd2 = rdd.distinct()
//4.行动算子
rdd2.collect.foreach(println)
//5.关闭SparkContext
sc.stop()

  }
}
