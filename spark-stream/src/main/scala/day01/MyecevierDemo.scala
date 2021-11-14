package day01

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

object MyecevierDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("MyecevierDemo")
    val ssc = new StreamingContext(conf, Seconds(3))
    val sourceStream = ssc.receiverStream(new Myecevier("hadoop002", 9999))
    val resulte = sourceStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    println(resulte)
    ssc.start()
    ssc.awaitTermination()
  }
}
class Myecevier(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  var socket:Socket = _
  var read:BufferedReader = _
  override def onStart(): Unit = {
    runInThread{
    try {
       socket = new Socket(host, port)
       read = new BufferedReader(new InputStreamReader(socket.getInputStream, "utf-8"))
      var line = read.readLine()
      while (line != null && socket.isConnected) {
        store(line)
        line = read.readLine()
      }
    }catch {
      case e => println(e.getMessage)
    }finally {
      restart("重启接收器")
    }
    }
  }
  def runInThread(op: =>Unit)={
    new Thread(){
      override def run(): Unit = op
    }.start()
  }

  override def onStop(): Unit = {
    if(socket!=null) socket.close()
    if(read!=null) read.close()
  }
}