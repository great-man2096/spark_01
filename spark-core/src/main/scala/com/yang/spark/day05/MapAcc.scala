package com.yang.spark.day05

import org.apache.spark.util.AccumulatorV2

case class MapAcc() extends AccumulatorV2[Double,Map[String,Any]] {
 private var map = Map[String,Any]()
  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[Double, Map[String, Any]] = {
    val acc = new MapAcc
    acc.map = map
    acc
  }

  override def reset(): Unit = map=Map[String,Any]()

  override def add(v: Double): Unit = {
   map +="sum" -> (map.getOrElse("sum",0D).asInstanceOf[Double] + v)
    map += "count" -> (map.getOrElse("count",0L).asInstanceOf[Long] + 1)
  }

  override def merge(other: AccumulatorV2[Double, Map[String, Any]]): Unit = {
    other match {
      case o:MapAcc =>
      map += "sum" -> (map.getOrElse("sum",0D).asInstanceOf[Double] + o.map.getOrElse("sum",0D).asInstanceOf[Double])
      map += "count" -> (map.getOrElse("count",0L).asInstanceOf[Long] + o.map.getOrElse("count",0L).asInstanceOf[Long])
      case _ => throw new UnsupportedOperationException
    }
  }

  override def value: Map[String, Any] = {
    map += "avg" -> (map.getOrElse("sum",0D).asInstanceOf[Double] / map.getOrElse("count",0L).asInstanceOf[Long])
    map
  }

}
