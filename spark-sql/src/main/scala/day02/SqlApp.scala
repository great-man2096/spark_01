package day02



import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

object SqlApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("SqlApp").master("local[2]")
      .getOrCreate()
    import spark.implicits._
    spark.sql("use mydb")
    spark.udf.register("remark",new CityRemarkUDAF)
    //	city_info	ci
    //	product_info	pi
    //	user_visit_action	uva
    spark.sql(
      """
        |select
        |ci.*,
        |pi.product_name,
        |uva.click_product_id
        |from user_visit_action uva
        |join product_info pi on uva.click_product_id=pi.product_id
        |join city_info ci on uva.city_id=ci.city_id
        |""".stripMargin).createOrReplaceTempView("t1")
    spark.sql(
      """
        |select
        |area,
        |product_name,
        |count(*) count,
        |remark(city_name) remark
        |from t1
        |group by area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")
    spark.sql(
      """
        |select
        |area,
        |product_name,
        |count,
        |remark,
        |rank() over(partition by area order by count desc) rk
        |from t2
        |""".stripMargin).createOrReplaceTempView("t3")
    val dff = spark.sql(
      """
        |select
        |area,
        |product_name,
        |count,
        |remark
        |from t3
        |where rk<=3
        |""".stripMargin)
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "123456")
    dff.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://hadoop002:3306/company?useUnicode=true&characterEncoding=utf8","sqlApp",properties)

    spark.close()
  }
}
/*
select
ci.*,
pi.product_name,
uva.click_product_id
from user_visit_action uva
join product_info pi on uva.click_product_id=pi.product_id
join city_info ci on uva.city_id=ci.city_id

select
area,
product_name,
count(*) count
from t1
group by area,product_name

select
area,
product_name,
count,
rank() over(partition by area order by count desc) rk
from t2

select
area,
product_name,
count
from t3
where rk<=3
 */