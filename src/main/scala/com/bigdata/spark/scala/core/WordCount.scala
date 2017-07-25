package com.bigdata.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  *
  * @@Author: CuiCan
  * @@Date: 2017-7-24
  * @@Time: 15:30
  * @@Description: scala 版本 wordcount
  */
object WordCount {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
//    val lines = sc.textFile("hdfs://devcluster/spark.txt")
//    //lines: org.apache.spark.rdd.RDD[String] = hdfs://devcluster/spark.txt MapPartitionsRDD[1]
//    val words = lines.flatMap(line => line.split(" "))
//    //words: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[2]
//    val pairs = words.map(word => (word, 1))
//    //pairs: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[3]
//    val wordCounts = pairs.reduceByKey(_ + _)
//    //wordCounts: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[4]

    val wordCounts = sc.textFile("hdfs://devcluster/spark.txt")
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    wordCounts.foreach(WordCount => println(WordCount._1 + " : " + WordCount._2))

  }
}
