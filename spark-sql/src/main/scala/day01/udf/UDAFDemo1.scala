package day01.udf

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

object UDAFDemo1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
          .builder()
          .appName("UDAFDemo").master("local[2]")
          .getOrCreate()
        import spark.implicits._
    val df = spark.read.json("C:\\tmp\\新建文本文档.txt")
        df.createOrReplaceTempView("users")
    //注册聚合函数
    spark.udf.register("myAvg",new MyAvg)
    spark.sql(
      """
        |select
        |myAvg(age)
        |from
        |users
        |""".stripMargin).show
        spark.close()
  }
}
class MyAvg extends UserDefinedAggregateFunction{
  //定义输入的数据类型
  override def inputSchema: StructType = StructType(StructField("ele",DoubleType)::Nil)
//定义缓冲区的类型
  override def bufferSchema: StructType = StructType(StructField("sum",DoubleType)::StructField("count",LongType)::Nil)
//最终聚合的数据类型
  override def dataType: DataType = DoubleType
//相同的输入是否返回相同的输出
  override def deterministic: Boolean = true
//对缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0D
    buffer(1)=0L
  }
//分区内聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){
      val v = input.getAs[Double](0)
      buffer(0) = buffer.getDouble(0) + v
      buffer(1) = buffer.getLong(1) + 1L
    }

  }
//分区间的聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }
//返回最终的输出值
  override def evaluate(buffer: Row): Any = buffer.getDouble(0)/buffer.getLong(1)
}