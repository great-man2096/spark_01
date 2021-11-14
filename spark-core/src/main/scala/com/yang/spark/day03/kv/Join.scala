package com.yang.spark.day03.kv

import org.apache.spark.{SparkConf, SparkContext}

object Join {
  def main(args: Array[String]): Unit = {
    //1.得到SparkContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("Join")
    val sc = new SparkContext(conf)
    //2.创建RDD
    var rdd1 = sc.parallelize(Array((1, "a"), (1, "b"), (2, "c")))
    var rdd2 = sc.parallelize(Array((1, "aa"), (3, "bb"), (2, "cc")))
    //3.转化
    //val rdd3 = rdd1.join(rdd2)//内链接
//    val rdd3 = rdd1.leftOuterJoin(rdd2)//左外链接
    val rdd3 = rdd1.fullOuterJoin(rdd2)//全外链接
    //4.行动算子
    rdd3.collect.foreach(println)
    //5.关闭SparkContext
    sc.stop()

  }
}
