package app

import bean.AdsInfo
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import util.RedisUtil

object LastHourApp extends App {
  override def doSomeThing(adsInfoStream: DStream[AdsInfo]): Unit = {
    adsInfoStream
      .window(Minutes(60), Seconds(3))
      .map(info => {
        (info.adsId, info.hmString) -> 1
      })
      .reduceByKey(_ + _)
      .map({
        case ((ads, hm), count) => (ads, (hm, count))
      })
      .groupByKey()
      .foreachRDD(rdd => {
        rdd.foreachPartition(it => {
          if(it.nonEmpty){
          val client = RedisUtil.getClient
          /* it.foreach({
             case (ads,hmcount)=>
               val key = "last:ads:hour:count"
               val field = ads
               import org.json4s.JsonDSL._
               val value = JsonMethods.compact(JsonMethods.render(hmcount))
               client.hset(key,field,value)
           })*/
          import org.json4s.JsonDSL._
          val key = "last:ads:hour:count"
          val map = it.toMap.map({
            case (str, tuples) => (str, JsonMethods.compact(JsonMethods.render(tuples)))
          })
          import scala.collection.JavaConversions._
          client.hmset(key, map)
          client.close()
          }
        })
      })
  }
}
