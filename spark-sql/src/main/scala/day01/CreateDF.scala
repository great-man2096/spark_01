package day01

import org.apache.spark.sql.SparkSession

object CreateDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CreatDF").master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val df = spark.read.json("C:\\tmp\\新建文本文档.txt")
    df.createOrReplaceTempView("users")
    spark.sql(
      """
        |select
        |name,age
        |from
        |users
        |""".stripMargin).show
    spark.stop()

  }
}
