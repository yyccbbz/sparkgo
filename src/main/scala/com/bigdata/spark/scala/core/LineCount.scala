package com.bigdata.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  *
  * Author: CuiCan
  * Date: 2017-7-28
  * Time: 16:37
  * Description: 统计每行出现次数
  */
object LineCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LineCount").setMaster("local")
    val sc = new SparkContext(conf)

    sc.textFile("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\txt\\hello.txt")
      .map(line => (line, 1)).reduceByKey(_ + _).foreach(t => println(t._1 + " 出现次数为 " + t._2))

  }

}
