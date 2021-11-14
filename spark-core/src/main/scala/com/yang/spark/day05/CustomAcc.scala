package com.yang.spark.day05

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

object CustomAcc {
  def main(args: Array[String]): Unit = {
  //1.得到SparkContext
  val conf = new SparkConf().setMaster("local[2]").setAppName("CustomAcc")
  val sc = new SparkContext(conf)
  //2.创建RDD
  val list = List(30,50,70,60,10,20)
  val rdd = sc.parallelize(list,2)
  //3.转化
  val acc = new MapAcc
    sc.register(acc)
  rdd.foreach(x=>{
    acc.add(x)
  })
    println(acc.value)
  //4.行动算子

  //5.关闭SparkContext
  sc.stop()

  }
}
case class MyAcc() extends AccumulatorV2[Int,Int] {
  private var sum = 0
  //对缓冲区值进行判“零”
  override def isZero: Boolean = sum == 0
//复制累加器
  override def copy(): AccumulatorV2[Int, Int] = {
    val acc = new MyAcc
    acc.sum = sum
    acc
  }
//重置累加器
  override def reset(): Unit = sum = 0
//真正的累加方法，分区内的累加
  override def add(v: Int): Unit = sum += v
//分区间的合并
  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    other match {
      case acc:MyAcc => this.sum += acc.sum
      case _ =>this.sum += 0
    }
  }
//返回累加后的最终值
  override def value: Int = sum
}