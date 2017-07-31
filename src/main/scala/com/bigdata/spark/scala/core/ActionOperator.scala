package com.bigdata.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  *
  * Author: CuiCan
  * Date: 2017-7-30
  * Time: 18:06
  * Description:
  */
object ActionOperator {

  def main(args: Array[String]): Unit = {

    countByKey()
    //saveAsTextFile()
    //take()
    //count()
    //collect()
    //reduce()
  }

  def reduce(): Unit = {
    val sc = new SparkContext(
      new SparkConf().setAppName("reduce").setMaster("local"))

    val count = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).reduce(_ + _)
    println(count)
  }

  def collect(): Unit = {
    val sc = new SparkContext(
      new SparkConf().setAppName("collect").setMaster("local"))

    val list = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map(n => n * 2).collect()
    for (n <- list) {
      println(n)
    }
  }

  def count() {
    val sc = new SparkContext(
      new SparkConf().setAppName("count").setMaster("local"))

    val count = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).count()
    println(count)
  }

  def take() {
    val sc = new SparkContext(
      new SparkConf().setAppName("take").setMaster("local"))

    val list = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).take(3)
    for (n <- list) {
      println(n)
    }
  }

  def saveAsTextFile() {
    val sc = new SparkContext(new SparkConf().setAppName("saveAsTextFile"))

    sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map(n => n * 5)
      .saveAsTextFile("hdfs://bd-cdh-master01:8020/double_number")
  }

  def countByKey() {
    val sc = new SparkContext(
      new SparkConf().setAppName("countByKey").setMaster("local"))

    val tuples = Array(("class1", "spark"), ("class2", "java"),
      ("class1", "kafka"), ("class2", "scala"), ("class1", "hadoop"))

    val map = sc.parallelize(tuples).countByKey()
    println(map)
//      .foreach(t => println(t._1 + " : " + t._2))
  }

}
