package day01

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}

object WordCount3 {
  val groupId = "111"
  val params = Map[String, String](
    "bootstrap.servers" -> "hadoop001:9092,hadoop002:9092,hadoop003:9092",
    "group.id"->groupId
  )
  val topics = Set("first111")
  val kafkaCluster = new KafkaCluster(params)

  def readOffsets() = {
    var resultMap = Map[TopicAndPartition,Long]()
    val topicAndPartitionSetEither = kafkaCluster.getPartitions(topics)
topicAndPartitionSetEither match {
  case Right(topicAndPartitionsSet) =>
    val topicAndPartitionOffsets = kafkaCluster.getConsumerOffsets(groupId, topicAndPartitionsSet)
    topicAndPartitionOffsets match {
      case Right(map) =>
        resultMap ++= map
      case _ =>
        topicAndPartitionsSet.foreach(topicAndPartition=>{
          resultMap += topicAndPartition -> 0L
        })
    }
  case _ =>
}
    resultMap
  }

  def saveOffsets(stream:InputDStream[String]) = {
    stream.foreachRDD(rdd=>{
      var map = Map[TopicAndPartition,Long]()
      val hasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
      val ranges = hasOffsetRanges.offsetRanges
      ranges.foreach(OffSetRange=>{
        val key = OffSetRange.topicAndPartition()
        val value = OffSetRange.untilOffset
        map += key->value
      })
      kafkaCluster.setConsumerOffsets(groupId,map)
    })

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCound1")
    val ssc = new StreamingContext(conf, Seconds(3))

    val sourceStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
      ssc,
      params,
      readOffsets(),
      (handler: MessageAndMetadata[String, String]) => handler.message()
    )
    sourceStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print(1000)
    saveOffsets(sourceStream)
    ssc.start()
    ssc.awaitTermination()
  }
}
