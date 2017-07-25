package com.bigdata.spark.scala.study

import scala.util.control.Breaks._


/**
  * Created with IntelliJ IDEA.
  *
  * @@Author: CuiCan
  * @@Date: 2017-7-25
  * @@Time: 9:59
  * @@Description: 循环语句
  */
object WhileDemo {

  def main(args: Array[String]): Unit = {
    //while do
    var n = 10
    //    while (n > 0) {
    //      print(n + " ")
    //      n -= 1
    //    }

    // for 循环
    // 简易版for语句
    for (i <- 1 to n) print(i + " ")

    println("----->>>>>")
    // 使用until，去上限
    for (i <- 1 until n) print(i + " ")

    println("----->>>>>")
    // 对字符串遍历，类似增强for
    for (c <- "Hello World") print(c + " ")

    println("----->>>>>")
    //跳出循环语句
    breakable {
      var n = 10
      for (c <- "Hello World") {
        if (n == 5) break;
        print(c)
        n -= 1
      }
    }

    println("----->>>>>")

    // 高级for循环
    //多重for循环：九九乘法表
    for (i <- 1 to 9; j <- 1 to 9) {
      if (j == 9) {
        println(i + "*" + j + "=" + i * j)
      } else {
        print(i + "*" + j + "=" + i * j + " ")
      }
    }
    //if守卫，取偶数
    for (i <- 1 to 100 if i % 2 == 0) println(i)
    //for推导式：构造集合
    // scala.collection.immutable.IndexedSeq[Int] = Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val ints = for (i <- 1 to 10) yield i
    println(ints)
  }

}
