package com.yang.spark.day05

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}

object JDBCWrite {
  def main(args: Array[String]): Unit = {
    //1.得到SparkContext
    val conf = new SparkConf().setMaster("local[2]").setAppName("DBCRead")
    val sc = new SparkContext(conf)
    val url = "jdbc:mysql://hadoop002:3306/company"
    val user = "root"
    val password = "123456"
    val rdd = sc.parallelize((20,"a")::(18,"b")::(22,"c")::Nil)


    val sql="insert into staff value(?,?)"
    /*rdd.foreach{

      case (age,name)=>{
        Class.forName("com.mysql.jdbc.Driver")
        val conn = DriverManager.getConnection(url, user, password)
        val ps = conn.prepareStatement(sql)
        ps.setInt(1,age)
        ps.setString(2,name)
        ps.execute()
        ps.close()
        conn.close()
      }
    }*/
    //分区
    /*rdd.foreachPartition(it =>{
      Class.forName("com.mysql.jdbc.Driver")
      val conn = DriverManager.getConnection(url, user, password)
      it.foreach{
        case (age,name)=>{
          val ps = conn.prepareStatement(sql)
          ps.setInt(1,age)
          ps.setString(2,name)
          ps.execute()
          ps.close()
        }
      }
      conn.close()
    })*/
    //批处理
    rdd.foreachPartition(it =>{
      Class.forName("com.mysql.jdbc.Driver")
      val conn = DriverManager.getConnection(url, user, password)
      val ps = conn.prepareStatement(sql)
      var count = 0
      it.foreach{
        case (age,name)=>{
          ps.setInt(1,age)
          ps.setString(2,name)
          ps.addBatch()
          count += 1
          if (count>=100) ps.executeBatch()
        }
          ps.executeBatch()
          ps.close()
      }
      conn.close()
    })

    sc.stop()
  }
}
