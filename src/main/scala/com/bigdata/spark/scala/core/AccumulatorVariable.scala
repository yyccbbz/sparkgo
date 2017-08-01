package com.bigdata.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  *
  * Author: CuiCan
  * Date: 2017-8-1
  * Time: 11:21
  * Description: 累加变量
  */
object AccumulatorVariable {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf().setAppName("accumulatorVariable").setMaster("local"))

    // 定义累加变量
    val sum = sc.accumulator(0)

    sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).foreach(n => sum.add(n))

    println(sum.value)

  }
}
