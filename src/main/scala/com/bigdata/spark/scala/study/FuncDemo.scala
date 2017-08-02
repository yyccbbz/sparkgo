package com.bigdata.spark.scala.study

/**
  * Created with IntelliJ IDEA.
  *
  * @@Author: CuiCan
  * @@Date: 2017-7-24
  * @@Time: 21:59
  * @@Description: 函数入门
  */
class FuncDemo {

}

object FuncDemo {

  //必须给出参数类型，返回值类型右侧只要不是递归语句，都可以自动类型推断
  def sayHello(name: String, age: Int) = {
    if (age > 18) {
      printf("hi %s,you are a big boy!!\n", name)
      age
    } else {
      printf("hi %s,you are a little boy!!\n", name)
      age
    }
  }

  //代码块中的函数体
  def sayHello1(name: String) = print("Hello," + name)

  //累加功能
  def sum(n: Int) = {
    var sum = 0
    for (i <- 1 to n) sum += i
    sum
  }

  //递归函数，必须手动指定返回值类型
  //经典的斐波那契数列
  def fab(n: Int): Int = {
    if (n <= 1) 1
    else fab(n - 1) + fab(n - 2)
  }

  //函数的默认参数
  def sayHello2(firstName: String,
                middleName: String = "Zinedine",
                lastName: String = "Zidane") = firstName + " " + middleName + " " + lastName

  //默认参数2
  def sayHello3(name: String, age: Int = 20): Unit = {
    print("Hello ," + name + ", your age is " + age)
  }

  //带名参数,调用函数时，指定函数参数名

  //变长参数
  def sum1(nums: Int*) = {
    var res = 0
    for (num <- nums) res += num
    res
  }

  /**
    * 使用序列调用变长参数
    *
    * 重要语法： val s = sum(1 to 5: _*)
    *
    * 递归函数实现累加
    */
  def sum3(nums: Int*): Int = {
    if (nums.length == 0) 0
    else nums.head + sum3(nums.tail: _*)
  }


  // 过程 过程通常用于不需要返回值的函数。
  def sayHello11(name: String) = "Hello, " + name
  def sayHello12(name: String) { print("Hello, " + name); "Hello, " + name }
  def sayHello13(name: String): Unit = "Hello, " + name


  // lazy


  // 异常

  def main(args: Array[String]): Unit = {



//    println(sum3(1 to 10: _*))
//    println(sum1(1, 2, 3, 4, 5))
//    printlsayHello2("Mick", lastName = "Nina", middleName = "Jack"))
//    println(sayHello2(firstName = "Mick", lastName = "Nina", middleName = "Jack"))
//    sayHe3("zizou")
//    println(sayHello2("zizou", "raule", "ruby"))
//    println(fab(10))
//    println(sum(10))
//    sayHello1("zidane")
//    println(sayHello("Leo",14))

  }

}
