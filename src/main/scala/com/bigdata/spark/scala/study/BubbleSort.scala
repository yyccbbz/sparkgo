package com.bigdata.spark.scala.study

/**
  * Created with IntelliJ IDEA.
  * Author: CuiCan
  * Date: 2017-8-15
  * Time: 11:55
  * Description: 冒泡排序
  */
object BubbleSort {

  // 外层循环做拆分
  def bubbleSort(l: List[Int]): List[Int] = l match {
    case List() => List()
    case head :: tail => bSort(head, bubbleSort(tail))
  }

  // 内层循环做排序
  def bSort(data: Int, dataSet: List[Int]): List[Int] = dataSet match {
    case List() => List(data)
    case head :: tail => if (data <= head) data :: dataSet else head :: bSort(data, tail)
  }

  def main(args: Array[String]) {
    val list = List(3, 12, 43, 23, 7, 1, 2, 20)
    println(bubbleSort(list))
  }

}
