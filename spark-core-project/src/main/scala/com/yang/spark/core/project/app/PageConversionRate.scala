package com.yang.spark.core.project.app

import java.text.DecimalFormat

import com.yang.spark.core.project.bean.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object PageConversionRate {
   def statPageConversionRate(sc:SparkContext,userVistActionRDD:RDD[UserVisitAction],pagesString:String)={
     val pages = pagesString.split(",")
     val prePages = pages.take(pages.length - 1)
     val postPages = pages.takeRight(pages.length - 1)
     val targetPageFlows = prePages.zip(postPages).map {
       case (pre, post) => s"$pre->$post"
     }
     val targetPageFlowsBC = sc.broadcast(targetPageFlows)
     val pageAndCount = userVistActionRDD
       .filter(action => prePages.contains(action.page_id.toString))
       .map(action => (action.page_id, 1))
       .countByKey()

     val sessionIdGrouped = userVistActionRDD.groupBy(_.session_id)
     val pageFlowsRDD = sessionIdGrouped.flatMap({
       case (sid, actionIt) =>
         val actions = actionIt.toList.sortBy(_.action_time)
         val preActions = actions.take(actions.length - 1)
         val postActions = actions.takeRight(actions.length - 1)
         preActions.zip(postActions).map {
           case (preAction, postAction) => s"${preAction.page_id}->${postAction.page_id}"
         }.filter(flow => targetPageFlowsBC.value.contains(flow))
     })
     val pageFlowsAndCount = pageFlowsRDD.map((_, 1)).countByKey()
     val f = new DecimalFormat(".00%")
     val result = pageFlowsAndCount.map({
       case (str, l) =>
         val rate = (str, l.toDouble / pageAndCount(str.split("->")(0).toLong))
         (str, f.format(rate))
     })
     println(result)
   }
}
