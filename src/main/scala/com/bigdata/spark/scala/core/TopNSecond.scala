package com.bigdata.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks._

/**
  * Created with IntelliJ IDEA.
  *
  * Author: CuiCan
  * Date: 2017-7-24
  * Time: 15:30
  * Description: TopN分组排序
  */
object TopNSecond {

  def main(args: Array[String]) {

    val sc = new SparkContext(
      new SparkConf().setAppName("TopNSecond by Scala").setMaster("local[1]"))

    // val data = sc.textFile("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\txt\\TopNSecond.txt")
    // data: RDD[String]
    val data = sc.textFile("E:\\Users\\IdeaProjects\\sparkgo\\src\\main\\resources\\txt\\TopNSecond.txt")

    //lines: RDD[(String, Int)]
    val lines = data.map(line => (line.split(" ")(0), line.split(" ")(1).toInt))

    // groups: RDD[(String, scala.Iterable[Int])]
    val groups = lines.groupByKey()

    // groupsSort: RDD[(String, List[Int])]
    val groupsSort = groups.map(t => {
      val key = t._1
      val values = t._2
      val sortValues = values.toList.sortWith(_ > _).take(4)
      (key, sortValues)
    })

    // res: RDD[(String, List[Int])]
    val res = groupsSort.sortBy(t => t._1, false, 1)

    res.collect().foreach(value => {
      print(value._1)
      value._2.foreach(v => print("\t" + v))
      println()
    })

    sc.stop()
  }
}
