package day01

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount2 {

  def createSSC() = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCound1")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("ck1")
    val params = Map[String, String](
      "bootstrap.servers" -> "hadoop001:9092,hadoop002:9092,hadoop003:9092",
      "group.id"->"111"
    )
    val sourceStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      params,
      Set("first111")
    ).map(x=>x._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
    ssc
  }

  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getActiveOrCreate("ck1", createSSC)
    ssc.start()
    ssc.awaitTermination()
  }
}
