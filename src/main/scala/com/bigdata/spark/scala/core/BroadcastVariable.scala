package com.bigdata.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  *
  * Author: CuiCan
  * Date: 2017-8-1
  * Time: 11:21
  * Description: 广播变量
  */
object BroadcastVariable {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf().setAppName("broadcastVariable").setMaster("local"))

    //定义广播变量
    val factor = 3
    val factorBroadcast = sc.broadcast(factor)

    sc.parallelize(1 to 10).map(n => n * factorBroadcast.value)
      .foreach(n => print(n + " , "))


  }
}
