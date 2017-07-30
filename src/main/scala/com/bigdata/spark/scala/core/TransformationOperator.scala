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
    //groupByKey()
    //reduceByKey()
    //sortByKey()
    //join();
    cogroup()
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

  def reduceByKey(): Unit = {
    val sc = new SparkContext(
      new SparkConf().setAppName("reduceByKey").setMaster("local"))

    val tuples = Array(("小一班", 89), ("小二班", 75), ("小二班", 94), ("小一班", 66))

    sc.parallelize(tuples)
      .reduceByKey(_ + _)
      .foreach(t => println(t._1 + " " + t._2))
  }

  def sortByKey(): Unit = {
    val sc = new SparkContext(
      new SparkConf().setAppName("sortByKey").setMaster("local"))

    val tuples = Array((89, "张三"), (75, "李四"), (94, "王五"), (66, "赵六"))

    sc.parallelize(tuples, 1)
      .sortByKey()
      .foreach(t => println(t._1 + " " + t._2))
  }

  def join(): Unit = {
    val sc = new SparkContext(
      new SparkConf().setAppName("join").setMaster("local"))

    val studentsList = Array((1, "张三"), (2, "李四"), (3, "王五"), (4, "赵六"))
    val scoresList = Array((1, 100), (2, 90), (3, 60), (4, 80))

    val students = sc.parallelize(studentsList, 1)
    val scores = sc.parallelize(scoresList, 1)

    val joinRDD = students.join(scores)
    joinRDD.foreach(t => {
      println("id = " + t._1)
      println("name = " + t._2._1)
      println("score = " + t._2._2)
      println("=============================")
    })
  }

  def cogroup(): Unit = {
    val sc = new SparkContext(
      new SparkConf().setAppName("cogroup").setMaster("local"))

    val studentsList = Array((1, "张三"), (2, "李四"), (3, "王五"), (4, "赵六"))
    val scoresList = Array((1, 100), (2, 90), (3, 60), (4, 80), (1, 50), (2, 40), (3, 30), (4, 20))

    val students = sc.parallelize(studentsList, 1)
    val scores = sc.parallelize(scoresList, 1)

    val joinRDD = students.cogroup(scores)
    joinRDD.foreach(t => {
      println("id = " + t._1)
      println("name = " + t._2._1)
      println("score = " + t._2._2)
      println("=============================")
    })
  }


}
