package day01

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val ssc = new StreamingContext(conf, Seconds(3))
    val sourceStream = ssc.socketTextStream("hadoop002", 9999)
    val result = sourceStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    result.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
