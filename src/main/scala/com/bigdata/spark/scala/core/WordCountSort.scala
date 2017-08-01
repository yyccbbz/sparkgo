package com.bigdata.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  *
  * @@Author: CuiCan
  * @@Date: 2017-7-24
  * @@Time: 15:30
  * @@Description: 排序的 wordcount
  */
object WordCountSort {

  def main(args: Array[String]) {

    val sc = new SparkContext(
      new SparkConf().setAppName("WordCountSort").setMaster("local"))

    val wordCounts = sc.textFile("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\txt\\spark.txt")
      .flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
      .map(t => (t._2, t._1)).sortByKey(false).map(t => (t._2, t._1))

    wordCounts.foreach(t => println(t._1 + " : " + t._2))

  }
}
