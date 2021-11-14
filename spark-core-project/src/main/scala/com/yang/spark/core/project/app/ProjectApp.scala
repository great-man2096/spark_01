package com.yang.spark.core.project.app

import com.yang.spark.core.project.bean.UserVisitAction
import org.apache.spark.{SparkConf, SparkContext}

object ProjectApp {
  def main(args: Array[String]): Unit = {
    //1.得到SparkContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("ProjectApp")
    val sc = new SparkContext(conf)
    //2.创建RDD

    val sourcerdd = sc.textFile("D:\\大数据学习\\15-spark【www.hoh0.com】\\spark-core数据\\user_visit_action.txt")

    //3.转化
    val rdd1 = sourcerdd.map(f = line => {
      val splits = line.split("_")
      UserVisitAction(
        splits(0),
        splits(1).toLong,
        splits(2),
        splits(3).toLong,
        splits(4),
        splits(5),
        splits(6).toLong,
        splits(7).toLong,
        splits(8),
        splits(9),
        splits(10),
        splits(11),
        splits(12).toLong)
    })
    //4.行动算子
    /*val categoryTop10 = CategoryTopApp.calcCategoryTop10(sc, rdd1)
    CategorySessionTopApp.statCategorySessionTop10_2(sc,categoryTop10,rdd1)*/
    PageConversionRate.statPageConversionRate(sc,rdd1,"1,2,3,4,5,6,7")
    //5.关闭SparkContext
    sc.stop()

  }
}
