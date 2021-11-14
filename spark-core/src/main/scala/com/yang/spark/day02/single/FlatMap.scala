package com.yang.spark.day02.single

import org.apache.spark.{SparkConf, SparkContext}

object FlatMap {
  def main(args: Array[String]): Unit = {
    //1.得到SparkContext
    val conf = new SparkConf().setMaster("local[3]").setAppName("FlatMap")
    val sc = new SparkContext(conf)
    //2.创建RDD
/*    val list1 = List(1 to 3,1 to 5,10 to 20)
    //val rdd = sc.parallelize(array)
    val rdd = sc.parallelize(list1,2)
    //3.转换
    val rdd2 = rdd.flatMap(x => x)*/
    val list = List(3, 5, 7, 6, 1, 2)
    val rdd1 = sc.parallelize(list)
   // val rdd2 = rdd1.flatMap(x => List(x, x * x, x * x * x))
   val rdd2 = rdd1.flatMap(x => if (x % 2 == 0) List(x, x * x, x * x * x) else List[Int]())
    rdd2.collect.foreach(println)
    //5.关闭SparkContext
    sc.stop()
  }
}
