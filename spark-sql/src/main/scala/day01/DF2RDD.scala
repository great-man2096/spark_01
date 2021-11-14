package day01

import org.apache.spark.sql.SparkSession

object DF2RDD {
  def main(args: Array[String]): Unit = {
        val spark = SparkSession
              .builder()
              .appName("DF2RDD").master("local[2]")
              .getOrCreate()
            import spark.implicits._
    val df = (1 to 10).toDF("number")
    df.printSchema()
    val rdd = df.rdd
    val rdd1 = rdd.map(row => row.getInt(0))
    rdd1.collect.foreach(println)
            spark.close()
  }
}
