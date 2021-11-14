package day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Window1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("TranformDemo1")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("ck2")
    val sourceStream = ssc.socketTextStream("hadoop002", 9999)

    val value = sourceStream.flatMap(_.split(" ").map((_, 1))).reduceByKeyAndWindow(_+_,_-_,Seconds(6),slideDuration=Seconds(3),filterFunc = _._2 >0)
    value.print
    ssc.start()
    ssc.awaitTermination()
  }
}
