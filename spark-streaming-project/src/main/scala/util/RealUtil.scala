package util

import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods

object RealUtil {
  implicit class MyRedis(result: DStream[((String, String), List[(String, Int)])] ){
    def saveToRedis={
      result.foreachRDD(rdd=>{
        rdd.foreachPartition(it=>{
          val client = RedisUtil.getClient
          it.foreach({
            //((2021-07-07,华中),List((4,9), (1,6), (2,6)))
            case ((day,area),adsCountList)=>
              val key = "area:ads:count"+day
              val field = area
              import org.json4s.JsonDSL._
              val value = JsonMethods.compact(JsonMethods.render(adsCountList))
              client.hset(key,field,value)
          })
          client.close()
        })
      })
    }
  }
}
