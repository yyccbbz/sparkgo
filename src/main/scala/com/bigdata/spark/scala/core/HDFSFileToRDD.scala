package com.bigdata.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  *
  * @@Author: CuiCan
  * @@Date: 2017-7-25
  * @@Time: 15:30
  * @@Description: scala 版本 使用hdfs文件创建RDD，统计文本文件字数
  */
object HDFSFileToRDD {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    val counts = sc.textFile("hdfs://devcluster/spark.txt", 3)
      .map(line => line.length).reduce(_ + _)

    println("文本文件总字数为：" + counts)
  }
}
