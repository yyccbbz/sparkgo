package com.bigdata.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  *
  * @@Author: CuiCan
  * @@Date: 2017-7-25
  * @@Time: 15:30
  * @@Description: scala 版本 并行化集合创建RDD
  */
object ParallelizeCollection {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("ParallelizeCollection").setMaster("local")
    val sc = new SparkContext(conf)

    //5个partition
    val counts = sc.parallelize(1 to 10, 5).reduce(_ + _)

    println(counts)

  }
}
