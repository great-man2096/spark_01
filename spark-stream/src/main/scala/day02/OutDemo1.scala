package day02

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OutDemo1 {
  val props = new Properties()
  props.setProperty("user","root")
  props.setProperty("password","123456")
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("OutDemo1")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("ck3")
    val sourceStream = ssc.socketTextStream("hadoop002", 9999)

    val value = sourceStream.flatMap(_.split(" ")).map((_,1)).updateStateByKey((seq:Seq[Int],opt:Option[Int])=>
      Some(seq.sum + opt.getOrElse(0))
    )

    value.foreachRDD(rdd =>{
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      rdd.toDF("word","count").write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://hadoop002:3306/company","rdd1",props)
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
