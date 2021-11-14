package com.yang.spark.core.project.app

import java.util

import com.yang.spark.core.project.bean.{CategoryCountInfo, SessionInfo, UserVisitAction}
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object CategorySessionTopApp {

  def statCategorySessionTop10(sc:SparkContext, categoryTop10:Array[CategoryCountInfo], userVisitActionRDD:RDD[UserVisitAction]): Unit ={
    val cids = categoryTop10.map(_.categoryId.toLong)
    val filteredUserVistActionRDD = userVisitActionRDD.filter(actioin => cids.contains(actioin.click_category_id))
    val cidSidAndOne = filteredUserVistActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
    val cidSidAndCount = cidSidAndOne.reduceByKey(_ + _)
    val cidAndSidCount = cidSidAndCount.map({
      case ((cid, sid), count) => (cid, (sid, count))
    })
    val cidAndSidCountItRDD = cidAndSidCount.groupByKey()
    cidAndSidCountItRDD.mapValues(it =>{
      it.toList.sortBy(-_._2).take(10)
    })
  }

  def statCategorySessionTop10_2(sc:SparkContext, categoryTop10:Array[CategoryCountInfo], userVisitActionRDD:RDD[UserVisitAction]): Unit ={
    val cids = categoryTop10.map(_.categoryId.toLong)
    val filteredUserVistActionRDD = userVisitActionRDD.filter(actioin => cids.contains(actioin.click_category_id))
    val cidSidAndOne = filteredUserVistActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
    val cidSidAndCount = cidSidAndOne.reduceByKey(_ + _)
    val cidAndSidCount = cidSidAndCount.map({
      case ((cid, sid), count) => (cid, (sid, count))
    })
    val cidAndSidCountItRDD = cidAndSidCount.groupByKey()
    val result = cidAndSidCountItRDD.mapValues(it => {
      var set = new mutable.TreeSet[SessionInfo]()
    it.foreach{
      case (str, i) =>
        val info = SessionInfo(str,i)
        set += info
        if(set.size > 10) set = set.take(10)
    }
    })
    result.collect.foreach(println)
  }

  def statCategorySessionTop10_3(sc:SparkContext, categoryTop10:Array[CategoryCountInfo], userVisitActionRDD:RDD[UserVisitAction]): Unit ={
    val cids = categoryTop10.map(_.categoryId.toLong)
    val filteredUserVistActionRDD = userVisitActionRDD.filter(actioin => cids.contains(actioin.click_category_id))
    val cidSidAndOne = filteredUserVistActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
    val cidSidAndCount = cidSidAndOne.reduceByKey(new CategorySessionPartitioner(cids),_ + _)
    val result = cidSidAndCount.mapPartitions(it=>{
      var set = new mutable.TreeSet[SessionInfo]()
      var categoryId=0L
      it.foreach{
        case ((str,str2), i) =>
          categoryId = str
          val info = SessionInfo(str2,i)
          set += info
          if(set.size > 10) set = set.take(10)
      }
      set.map((categoryId,_)).toIterator
    })

    result.collect.foreach(println)
  }


  def statCategorySessionTop10_1(sc:SparkContext, categoryTop10:Array[CategoryCountInfo], userVisitActionRDD:RDD[UserVisitAction]): Unit ={
    val cids = categoryTop10.map(_.categoryId.toLong)
    val filteredUserVistActionRDD = userVisitActionRDD.filter(actioin => cids.contains(actioin.click_category_id))
    cids.foreach(cid=>{
      val cidUserVisitActionRDD = filteredUserVistActionRDD.filter(_.click_category_id == cid)
      val r = cidUserVisitActionRDD
        .map(action => ((action.click_category_id, action.session_id), 1))
        .reduceByKey(_ + _)
        .map({
          case ((cid, sid), count) => (cid, (sid, count))
        })
        .sortBy(-_._2._2)
        .take(10)
          .groupBy(_._1)
          .map({
            case (l, tuples) => (l,tuples.map(_._2).toList)
          })
      println(r.toMap)
    })
  }
}
class CategorySessionPartitioner(cids:Array[Long]) extends Partitioner {
  private val cidIndexMap: Map[Long, Int] = cids.zipWithIndex.toMap

  override def numPartitions: Int = cids.length

  override def getPartition(key: Any): Int = key match {
    case (cid:Long,_)=>cidIndexMap(cid)
  }
}