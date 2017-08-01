package com.bigdata.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  *
  * Author: CuiCan
  * Date: 2017-7-24
  * Time: 15:30
  * Description: top3
  */
object Top3 {

  def main(args: Array[String]) {

    val sc = new SparkContext(
      new SparkConf().setAppName("WordCountSort").setMaster("local"))

    val top3 = sc.textFile("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\txt\\top.txt")
      .map(line => (line, line)).sortByKey(false).map(t => t._1).take(3)
    for (s <- top3) {
      println(s)
    }

  }
}
