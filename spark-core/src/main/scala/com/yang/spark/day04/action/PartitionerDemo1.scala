package com.yang.spark.day04.action

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object PartitionerDemo1 {
  def main(args: Array[String]): Unit = {
    //1.得到SparkContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("PartitionerDemo1")
    val sc = new SparkContext(conf)
    //2.创建RDD
    val list = List(30, 50, 70, 60, 10, 20, null, null)
    val rdd = sc.parallelize(list, 4).map((_, 1))
    //3.转化
    val rdd2 = rdd.partitionBy(MyPartitioner(3))
    //4.行动算子
    rdd2.glom().map(_.toList).collect().foreach(println)
    //5.关闭SparkContext
    sc.stop()

  }
}

case class MyPartitioner(num: Int) extends Partitioner {
  assert(num > 0)//如果不是大于零的数就会抛异常

  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => key.hashCode().abs % num
  }
}