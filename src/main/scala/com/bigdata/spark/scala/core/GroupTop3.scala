package com.bigdata.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks._

/**
  * Created with IntelliJ IDEA.
  *
  * Author: CuiCan
  * Date: 2017-7-24
  * Time: 15:30
  * Description: 分组top3
  */
object GroupTop3 {

  def main(args: Array[String]) {

    val sc = new SparkContext(
      new SparkConf().setAppName("WordCountSort").setMaster("local[1]"))

    //    val groupPairs = sc.textFile("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\txt\\score.txt")

    // groupPairs: RDD[(String, scala.Iterable[Int])]
    val groupPairs = sc.textFile("E:\\Users\\IdeaProjects\\sparkgo\\src\\main\\resources\\txt\\score.txt")
      .map(line => (line.split(" ")(0), line.split(" ")(1).toInt)).groupByKey()

    //top3: RDD[(String, List[Int])]
    val top3 = groupPairs.map(t => {
      val className = t._1
      val scores = t._2
      val sortScores = scores.toList.sortWith(_ > _).take(3)
      (className, sortScores)
    })

    top3.foreach(t => {
      print(t._1)
      t._2.foreach(s => print("\t" + s))
      println()
    })
    /*val top3Score = groupPairs.map(classScore => {

      val top3 = Array[Int](-1, -1, -1)

      val className = classScore._1

      val sores = classScore._2

      for (score <- sores) {
        breakable {

          for (i <- 0 until 3) {
            if (top3(i) == -1) {
              top3(i) = score
              break;
            } else if (score > top3(i)) {
              var j = 2
              while (j > i) {
                top3(j) = top3(j - 1)
                j = j - 1
              }
              top3(i) = score
              break;
            }
          }

        }
      }
      (className, top3);
    })

    top3Score.foreach(t => {
      println(t._1)
      for (s <- t._2) {
        println(s)
      }
      println("======================")
    })*/
  }
}
