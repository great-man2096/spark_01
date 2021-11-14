package app



import bean.AdsInfo
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.MyKafkaUtils


trait App {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("App")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("ck1")
    val sourceStream = MyKafkaUtils.getKafkaSteam(ssc, "ads_log111")
    val adsInfoStream: DStream[AdsInfo] = sourceStream.map(s => {
      val spilts = s.split(",")
      AdsInfo(spilts(0).toLong, spilts(1), spilts(2), spilts(3), spilts(4))
    })
    doSomeThing(adsInfoStream)
    ssc.start()
    ssc.awaitTermination()
  }
  def doSomeThing(adsInfoStream: DStream[AdsInfo]):Unit
}
