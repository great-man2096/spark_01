package com.yang.spark.day04.action

import org.apache.spark.{SparkConf, SparkContext}

object CheckPoint {
  def main(args: Array[String]): Unit = {
    //1.得到SparkContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("CheckPoint")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("./ck1")
    //2.创建RDD
    val list = List(30,50,70,60,10,20)
    val rdd = sc.parallelize(list,2)
    //3.转化
    val rdd2 = rdd.map(x => {
      println("map:" + x)
      x
    })
    val rdd3 = rdd2.filter(x => {
      println("filter" + x)
      true
    })
    rdd3.checkpoint()//1.第一个job执行完之后，spark内部会立即再起一个同类型的job专门做checkpoint2.这个方法会切割血缘关系
    rdd3.cache()//配合checkpoint使用减少运算次数
    //4.行动算子
    rdd3.collect
    println("==========")
    rdd3.collect
    //5.关闭SparkContext
    sc.stop()

  }
}
