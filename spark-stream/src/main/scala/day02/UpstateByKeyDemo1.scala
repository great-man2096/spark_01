package day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpstateByKeyDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("UpstateByKeyDemo1")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("ck1")
    val sourceStream = ssc.socketTextStream("hadoop002", 9999)

    val value = sourceStream.flatMap(_.split(" ").map((_, 1))).updateStateByKey((seq:Seq[Int],opt:Option[Int])=>{
      Some(seq.sum + opt.getOrElse(0))
    }).print(1000)
    ssc.start()
    ssc.awaitTermination()
  }
}
