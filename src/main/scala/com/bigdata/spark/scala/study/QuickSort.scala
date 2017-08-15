package com.bigdata.spark.scala.study

/**
  * Created with IntelliJ IDEA.
  * Author: CuiCan
  * Date: 2017-8-15
  * Time: 11:56
  * Description: 快速排序
  */
object QuickSort {

  def quickSort(list: List[Int]): List[Int] = list match {
    case Nil => Nil
    case List() => List()
    case head :: tail =>
      // 数组，按照首元素，分成左右2个partition
      val (left, right) = tail.partition(_ < head)
      quickSort(left) ::: head :: quickSort(right)
  }

  def main(args: Array[String]) {
    val list = List(3, 12, 43, 23, 7, 1, 2, 20)
    println(quickSort(list))
  }

}
