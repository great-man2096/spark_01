package day01

import day01.RDD2DF.User
import org.apache.spark.sql.SparkSession

object DFDS {
  def main(args: Array[String]): Unit = {

        val spark = SparkSession
              .builder()
              .appName("DFDS").master("local[2]")
              .getOrCreate()
            import spark.implicits._
    val ints = List(User("a",1),User("b",2))
    val df = ints.toDF()
    val ds = df.as[User]
    val df1 = ds.toDF()

            spark.close()
  }
}
