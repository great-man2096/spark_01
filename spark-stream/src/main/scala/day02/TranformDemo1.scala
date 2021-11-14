package day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TranformDemo1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("TranformDemo1")
    val ssc = new StreamingContext(conf, Seconds(3))
    val sourceStream = ssc.socketTextStream("hadoop002", 9999)
    val value = sourceStream.transform(rdd => {
      rdd.flatMap(_.split(" ").map((_, 1))).reduceByKey(_ + _)
    })
    value.print
    ssc.start()
    ssc.awaitTermination()
  }
}
