package com.bigdata.spark.scala.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Author: CuiCan
  * Date: 2017-8-15
  * Time: 18:26
  * Description: scala中使用反射的方式将RDD转换为DataFrame
  * 需要手动导入一个隐式转换
  * extends App的方式
  */
object RDD2DataFrameReflection extends App {

  val conf = new SparkConf().setAppName("RDD2DataFrameReflection").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  case class Student(id: Int, name: String, age: Int)

  val lines = sc.textFile("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\txt\\students.txt")
  val stuednets = lines.map(line => line.split(","))
    .map(arr => Student(arr(0).trim.toInt, arr(1), arr(2).trim.toInt))

  // 反射转换 toDF()
  import sqlContext.implicits._

  val studentDF = stuednets.toDF()

  studentDF.registerTempTable("students")

  val teenagerDF = sqlContext.sql("select * from students where age>=18")

  val teenagerRDD = teenagerDF.rdd

  // scala中 row的顺序不会打乱，且对row的计算更加丰富
  teenagerRDD.map(row => Student(row(0).toString.toInt, row(1).toString, row(2).toString.toInt))
    .collect()
    .foreach(s => println(s.id + " : " + s.name + " : " + s.age))

  // row的getAs()方法，获取指定列名的列
   teenagerRDD.map(row => Student(row.getAs[Int]("id"),row.getAs[String]("name"),row.getAs[Int]("age")))
     .collect()
     .foreach(s => println(s.id + " : " + s.name + " : " + s.age))

  // 还可以通过 row的getValuesMap()方法，获取指定的几列的值，返回的是个Map
  teenagerRDD.map(row => {
    // map: Map[String, Any]
    val map = row.getValuesMap[Any](Array("id", "name", "age"))
    Student(map("id").toString.toInt, map("name").toString, map("age").toString.toInt)
  }).collect().foreach(s => println(s.id + " : " + s.name + " : " + s.age))


}
