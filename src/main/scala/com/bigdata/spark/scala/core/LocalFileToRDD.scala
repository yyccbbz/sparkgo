package com.bigdata.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  *
  * @@Author: CuiCan
  * @@Date: 2017-7-25
  * @@Time: 15:30
  * @@Description: scala 版本 使用本地文件创建RDD，统计文本文件字数
  */
object LocalFileToRDD {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    val counts = sc.textFile("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\txt\\spark.txt")
      .map(line => line.length).reduce(_ + _)

    println("文本文件总字数为：" + counts)
  }
}
