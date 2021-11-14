package com.yang.spark.core.project.bean

case class SessionInfo(sessionId:String,count:Long) extends Ordered[SessionInfo]{
  override def compare(that: SessionInfo): Int = if(that.count<this.count) -1  else 1

}
