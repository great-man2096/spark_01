package day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Window2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("TranformDemo1")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("ck2")
    val sourceStream = ssc.socketTextStream("hadoop002", 9999)

    val value = sourceStream.window(Seconds(9),Seconds(6)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    value.saveAsTextFiles("yang","log")
    ssc.start()
    ssc.awaitTermination()
  }
}
