package com.yang.spark.day02.single

import org.apache.spark.{SparkConf, SparkContext}

object Filter {
  def main(args: Array[String]): Unit = {
     //1.得到SparkContext
     val conf = new SparkConf().setMaster("local[3]").setAppName("Filter")
     val sc = new SparkContext(conf)
     //2.创建RDD
     val array = Array(30,50,70,60,10,20)
     //val rdd = sc.parallelize(array)
     val rdd = sc.makeRDD(array)
    //3.转化
    val rdd2 = rdd.filter(x => x > 20)
     //4.行动算子
     rdd2.collect.foreach(println)
     //5.关闭SparkContext
     sc.stop()


  }
}
