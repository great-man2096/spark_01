package app
import bean.AdsInfo
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import util.RedisUtil

object AreaTopApp extends App {
  override def doSomeThing(adsInfoStream: DStream[AdsInfo]): Unit = {
    val dayAreaGrouped = adsInfoStream
      .map(adsInfo => ((adsInfo.dayString, adsInfo.area, adsInfo.adsId), 1))
      .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
        Some(seq.sum + opt.getOrElse(0))
      })
      .map {
        case ((day, area, ads), count) => ((day, area), (ads, count))
      }
      .groupByKey()

    val result: DStream[((String, String), List[(String, Int)])] = dayAreaGrouped.map({
      case (tuple, tuples) =>
        (tuple, tuples.toList.sortBy(-_._2).take(3))
    })
   // result.print(100)
   /* result.foreachRDD(rdd=>{
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
    })*/
    import util.RealUtil._
    result.saveToRedis
  }
}
