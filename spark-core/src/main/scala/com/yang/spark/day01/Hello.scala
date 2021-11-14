package com.yang.spark.day01

import org.apache.spark.{SparkConf, SparkContext}

object Hello {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Hello")
    val sc = new SparkContext(conf)

    val lineRDD = sc.textFile(args(0))
    val resultRDD = lineRDD.flatMap(_.split("\\W+")).map((_, 1)).reduceByKey(_ + _)

    val wordcountArr = resultRDD.collect()
    wordcountArr.foreach(println)

    sc.stop()
  }
}
