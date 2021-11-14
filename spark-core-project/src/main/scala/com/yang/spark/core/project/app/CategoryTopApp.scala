package com.yang.spark.core.project.app

import com.yang.spark.core.project.acc.CategoryAcc
import com.yang.spark.core.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object CategoryTopApp {

  def calcCategoryTop10(sc:SparkContext, userVisitActionRDD:RDD[UserVisitAction]) ={
    val acc = new CategoryAcc
    sc.register(acc)
    userVisitActionRDD.foreach(action=> acc.add(action))
    val cidActionCountGrouped = acc.value.groupBy(_._1._1)
    val categoryCountInforArray = cidActionCountGrouped.map {
      case (cid, map) =>
        CategoryCountInfo(cid,
          map.getOrElse((cid, "click"), 0L),
          map.getOrElse((cid, "order"), 0L),
          map.getOrElse((cid, "pay"), 0L))
    }.toArray
    val result = categoryCountInforArray.sortBy(info => (-info.clickCount, -info.orderCount, -info.payCount)).take(10)
   // result.foreach(println)
    result
  }
}
