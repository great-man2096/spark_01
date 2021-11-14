package com.yang.spark.day02.single

import org.apache.spark.{SparkConf, SparkContext}

object Sample {
  def main(args: Array[String]): Unit = {
//1.得到SparkContext
val conf = new SparkConf().setMaster("local[2]").setAppName("Sample")
val sc = new SparkContext(conf)
//2.创建RDD
val array = 1 to 20
val rdd = sc.parallelize(array,2)
//3.转化
val rdd2 = rdd.sample(false, 0.2)//flase表示不放回
//4.行动算子
rdd2.collect.foreach(println)
//5.关闭SparkContext
sc.stop()

  }
}
