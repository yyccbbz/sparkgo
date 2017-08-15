package com.bigdata.spark.scala.study

/**
  * Created with IntelliJ IDEA.
  * Author: CuiCan
  * Date: 2017-8-15
  * Time: 11:57
  * Description: 归并排序
  */
object MergeSort {

  def mergedSort[T](less: (T, T) => Boolean)(list: List[T]): List[T] = {

    // 找到第一个最小的值，然后递归的进行比较（less只是一个def函数）
    def merged(xList: List[T], yList: List[T]): List[T] = {
      (xList, yList) match {
        case (Nil, _) => yList
        case (_, Nil) => xList
        case (x :: xTail, y :: yTail) => {
          if (less(x, y)) x :: merged(xTail, yList)
          else
            y :: merged(xList, yTail)
        }
      }
    }

    val n = list.length / 2
    if (n == 0) list
    else {
      // splitAt 从n个元素开始切分，把数据分为2份
      val (x, y) = list splitAt n
      // 合并左右的数组
      merged(mergedSort(less)(x), mergedSort(less)(y))
    }
  }

  def main(args: Array[String]) {
    val list = List(1, 2, 3, 7, 12, 20, 23, 43)
    println(mergedSort((x: Int, y: Int) => x < y)(list))
  }

}
