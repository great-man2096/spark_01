package com.yang.spark.day03.kv

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PatitionBy {
  def main(args: Array[String]): Unit = {
//1.得到SparkContext
val conf = new SparkConf().setMaster("local[2]").setAppName("PatitionBy")
val sc = new SparkContext(conf)
//2.创建RDD
val list = List(30,50,70,60,10,20)
val rdd = sc.parallelize(list,2)
//3.转化

val rdd2 = rdd.zipWithIndex()
   /* println(rdd2.partitioner)
    val rdd3 = rdd2.partitionBy(new HashPartitioner(3))
    println(rdd3.partitioner)*/
   val rdd3 = rdd2.map {
     case (k, v) => (v, k)
   }.partitionBy(new HashPartitioner(5)).map{
     case (v,k)=>(k,v)
   }
rdd3.glom().collect.map(_.toList).foreach(println)
//5.关闭SparkContext
sc.stop()

  }
}
