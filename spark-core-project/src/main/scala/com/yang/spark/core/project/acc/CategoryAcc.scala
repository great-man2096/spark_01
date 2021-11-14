package com.yang.spark.core.project.acc

import com.yang.spark.core.project.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

class CategoryAcc extends AccumulatorV2[UserVisitAction,Map[(String,String),Long]]{
  self =>
  //自动义空的缓冲Map
  private var map = Map[(String,String),Long]()
  //判断累加器是否为空
  override def isZero: Boolean = map.isEmpty
//复制累加器
  override def copy(): AccumulatorV2[UserVisitAction, Map[(String, String), Long]] = {
    val acc = new CategoryAcc
    acc.map = map
    acc
  }
//重置累加器  这个方法调用完之后，isZero必须返回ture
  override def reset(): Unit = map = Map[(String,String),Long]()
//分区内累加
  override def add(v: UserVisitAction): Unit = {
    //分别计算三个指标
    v match {
      case action if action.click_category_id != -1 => //点击行为
        val key = (action.click_category_id.toString, "click")
        map += key -> (map.getOrElse(key,0L) + 1L)
      case action if action.order_category_ids != "null" => //下单行为
        val cIds = action.order_category_ids.split(",")
        cIds.foreach(cid=>{
          val key = (cid,"order")
          map += key->(map.getOrElse(key,0L) + 1L)
        })
      case action if action.pay_category_ids != "null" => //支付行为
        val cIds = action.pay_category_ids.split(",")
        cIds.foreach(cid=>{
          val key = (cid,"pay")
          map += key->(map.getOrElse(key,0L) + 1L)
        })
      case _ => //其他非正常情况
    }
  }
//分区间的合并
  override def merge(other: AccumulatorV2[UserVisitAction, Map[(String, String), Long]]): Unit = {
    other match {
      case o:CategoryAcc =>
        o.map.foreach{
          case ((cid,action),count) =>
            self.map += (cid,action)->(self.map.getOrElse((cid,action),0L)+count)
        }
      case _=>
        throw new UnsupportedOperationException
    }
  }
//最终的返回值
  override def value: Map[(String, String), Long] = map
}
