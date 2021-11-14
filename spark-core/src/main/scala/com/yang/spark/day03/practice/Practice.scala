package com.yang.spark.day03.practice

import org.apache.spark.{SparkConf, SparkContext}

/*
需求
1.数据结构：时间戳，省份，城市，用户，广告，字段使用空格分割。
1516609143867 6 7 64 16
1516609143869 9 4 75 18
1516609143869 1 7 87 12
2.需求: 统计出每一个省份广告被点击次数的 TOP3
 */
object Practice {
  def main(args: Array[String]): Unit = {
    //1.得到SparkContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("Practice")
    val sc = new SparkContext(conf)
    //2.创建RDD
    val lineRDD = sc.textFile("D:\\大数据学习\\15-spark【www.hoh0.com】\\agent.log")
    //3.转化
    val one = lineRDD.map(line => {
      val splits = line.split(" ")
      ((splits(1), splits(4)), 1)
    })
    val two = one.reduceByKey(_ + _)
    val three = two.map({
      case (a, b) => (a._1, (a._2, b))
    })
    val four = three.groupByKey().mapValues(_.toList)
    val five = four.map({
      case (a, b) => {
        (a, b.sortBy(_._2)(Ordering.Int.reverse).take(3))
      }
    })
    val six = five.sortBy(_._1.toInt)
    //4.行动算子
    six.collect.foreach(println)
    //5.关闭SparkContext
    sc.stop()

  }
}
