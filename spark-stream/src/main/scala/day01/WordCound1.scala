package day01

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCound1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCound1")
    val ssc = new StreamingContext(conf, Seconds(3))
    val params = Map[String, String](
      "bootstrap.servers" -> "hadoop001:9092,hadoop002:9092,hadoop003:9092",
      "group.id"->"111"
    )
    val sourceStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      params,
      Set("first111")
    ).map(x=>x._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
