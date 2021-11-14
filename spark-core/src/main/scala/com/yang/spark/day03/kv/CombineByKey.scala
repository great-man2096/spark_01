package com.yang.spark.day03.kv

import org.apache.spark.{SparkConf, SparkContext}

object CombineByKey {
  def main(args: Array[String]): Unit = {
    //1.得到SparkContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("ReduceByKey")
    val sc = new SparkContext(conf)
    //2.创建RDD
    val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("b", 4), ("d", 3), ("y", 6), ("a", 8), ("d", 3)), 3)
    //3.转化
    val rdd2 = rdd.combineByKey(
      v => v,
      (c: Int, v) => c + v,
      (c1: Int, c2: Int) => c1 + c2
    )
    val rdd3 = rdd.combineByKey(
      v => v,
      (max: Int, v) => max.max(v),
      (max1: Int, max2: Int) => max1 + max2
    )
    val rdd4 = rdd.combineByKey(
      (v:Int) => (v, 1),
      (sum:(Int,Int),v:Int)=>(sum._1+v,sum._2+1),
      (sum1:(Int,Int),sum2:(Int,Int))=>(sum1._1+sum2._1,sum1._2+sum2._2)
    ).mapValues(x => x._1.toDouble / x._2)
    //4.行动算子
    rdd2.collect.foreach(println)
    println("=======================")
    rdd4.collect.foreach(println)
    //5.关闭SparkContext
    sc.stop()
  }
}
