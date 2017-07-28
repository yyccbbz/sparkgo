package com.bigdata.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  *
  * Author: CuiCan
  * Date: 2017-7-28
  * Time: 16:37
  * Description: transformation 算子实战
  */
object TransformationOperator {

  def main(args: Array[String]): Unit = {

//    map()
//    filter()
//    flatMap()
    groupByKey()



  }

  def map(): Unit = {
    val sc = new SparkContext(
      new SparkConf().setAppName("map").setMaster("local"))

    val rdd = sc.parallelize(Array(1, 2, 3, 4, 5)).map(n => n * 2)
    rdd.foreach(n => println(n))
  }

  def filter(): Unit = {
    val sc = new SparkContext(
      new SparkConf().setAppName("filter").setMaster("local"))

    val rdd = sc.parallelize(Array(1, 2, 3, 4, 5)).filter(x => x % 2 == 0)
    rdd.foreach(n => println(n))
  }

  def flatMap(): Unit = {
    val sc = new SparkContext(
      new SparkConf().setAppName("flatMap").setMaster("local"))

    val strings = Array("hello java", "hello scala", "hello spark")

    sc.parallelize(strings)
      .flatMap(line => line.split(" "))
      .foreach(word => println(word))
  }

  def groupByKey(): Unit = {
    val sc = new SparkContext(
      new SparkConf().setAppName("groupByKey").setMaster("local"))

    val tuples = Array(("小一班", 89), ("小二班", 75), ("小二班", 94), ("小一班", 66))

    sc.parallelize(tuples)
      .groupByKey()
      .foreach(t => {
        println(t._1)
        t._2.foreach(s => println(s))
        println("=====================")
      })
  }





}
