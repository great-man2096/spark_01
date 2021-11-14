package day02

import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}

class CityRemarkUDAF extends UserDefinedAggregateFunction {
  //输入类型
  override def inputSchema: StructType = StructType(Array(StructField("city", StringType)))

  //缓冲类型
  override def bufferSchema: StructType =
    StructType(Array(StructField("map", MapType(StringType, LongType)), StructField("total", LongType)))

  //聚合结果的类型
  override def dataType: DataType = StringType

  //确定性
  override def deterministic: Boolean = true

  //对缓冲区做初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Long]()
    buffer(1) = 0L
  }

  //分区内聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    input match {
      case Row(cityName: String) =>
        buffer(1) = buffer.getLong(1) + 1L
        val map = buffer.getMap[String, Long](0)
        buffer(0) = map + (cityName -> (map.getOrElse(cityName, 0L) + 1L))
      case _ =>
    }
  }

  //分区聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1 = buffer1.getMap[String, Long](0)
    val map2 = buffer2.getMap[String, Long](0)
    val l1 = buffer1.getLong(1)
    val l2 = buffer2.getLong(1)
    buffer1(1) = l1 + l2
    buffer1(0) = map1.foldLeft(map2) {
      case (map, (cityName, count)) =>
        map + (cityName -> (map.getOrElse(cityName, 0L) + count))
    }
  }

  //最终输出结果
  override def evaluate(buffer: Row): String = {
    val cityAndCount = buffer.getMap[String, Long](0)
    val total = buffer.getLong(1)
    val cityCountTop2 = cityAndCount.toList.sortBy(-_._2).take(2)
    var cityRemarks = cityCountTop2.map {
      case (str, l) => CityRemark(str, l.toDouble / total)
    }
    cityRemarks :+= CityRemark("其他", cityRemarks.foldLeft(1D)(_ - _.cityRadio))
    cityRemarks.mkString(",")
  }
}

case class CityRemark(cityName: String, cityRadio: Double){
  val f = new DecimalFormat("0.00%")
  override def toString = s"$cityName:${f.format(cityRadio.abs)}"
}