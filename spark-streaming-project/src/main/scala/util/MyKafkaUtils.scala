package util

import java.lang

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object MyKafkaUtils {
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "hadoop001:9092,hadoop002:9092,hadoop003:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "yang111",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: lang.Boolean)
  )
  def getKafkaSteam(ssc:StreamingContext,topics:String*) ={
    KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics.toIterable, kafkaParams)
    ).map(_.value())
  }
}
