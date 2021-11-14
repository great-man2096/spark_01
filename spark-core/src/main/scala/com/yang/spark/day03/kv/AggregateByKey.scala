package com.yang.spark.day03.kv

import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKey {
  def main(args: Array[String]): Unit = {
    //1.得到SparkContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("ReduceByKey")
    val sc = new SparkContext(conf)
    //2.创建RDD
    val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("b", 4), ("d", 3), ("y", 6), ("a", 8), ("d", 3)), 3)
    //3.转化
    val rdd2
    = rdd.aggregateByKey(Int.MinValue)(_.max(_), _ + _)
    val rdd3 = rdd.aggregateByKey((0, 0))({
      case ((sum, count), v) => (sum + v, count + 1)
    }, {
      case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)
    }).mapValues(x => x._1.toDouble / x._2)
    //4.行动算子
    rdd2.collect.foreach(println)
    println("===========================================")
    rdd3.collect.foreach(println)
    //5.关闭SparkContext
    sc.stop()
  }
}
