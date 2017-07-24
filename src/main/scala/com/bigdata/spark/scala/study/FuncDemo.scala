package com.bigdata.spark.scala.study

/**
  * Created with IntelliJ IDEA.
  *
  * @@Author: CuiCan
  * @@Date: 2017-7-24
  * @@Time: 21:59
  * @@Description:
  */
class FuncDemo {

}

object FuncDemo {

  def sayHello(name: String, age: Int) = {
    if (age > 18) {
      printf("hi %s,you are a big boy!!\n", name)
      age
    } else {
      printf("hi %s,you are a little boy!!\n", name)
      age
    }
  }

  def main(args: Array[String]): Unit = {

    val res = sayHello("Leo",14)
    println(res)
  }


}