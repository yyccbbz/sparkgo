package com.bigdata.spark.scala.core

import com.bigdata.spark.java.core.SecondarySortKey
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  *
  * Author: CuiCan
  * Date: 2017-7-24
  * Time: 15:30
  * Description: 二次排序
  */
object SecondarySort {

  def main(args: Array[String]) {

    val sc = new SparkContext(
      new SparkConf().setAppName("WordCountSort").setMaster("local"))

    val sorted = sc.textFile("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\txt\\sort.txt")
      .map(line => (new SecondarySortKey(line.split(" ")(0).toInt, line.split(" ")(1).toInt), line))
      .sortByKey().map(t => t._2)
    sorted.foreach(s => println(s))

  }
}
