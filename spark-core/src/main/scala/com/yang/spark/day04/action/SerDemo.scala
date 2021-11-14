package com.yang.spark.day04.action

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.{SparkConf, SparkContext}

object SerDemo {
  def main(args: Array[String]): Unit = {
//1.得到SparkContext
val conf = new SparkConf().setMaster("local[2]").setAppName("SerDemo")
  .set("spark.serializer",classOf[Kryo].getName)
  .registerKryoClasses(Array(classOf[Searcher]))

val sc = new SparkContext(conf)
//2.创建RDD
val list = List(30,50,70,60,10,20)
val rdd = sc.parallelize(list,2)
//3.转化
val searcher = new Searcher("hello")
//4.行动算子
//5.关闭SparkContext
sc.stop()

  }
}

case class Searcher(str: String)
