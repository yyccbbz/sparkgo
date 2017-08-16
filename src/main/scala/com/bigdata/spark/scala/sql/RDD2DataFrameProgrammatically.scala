package com.bigdata.spark.scala.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Author: CuiCan
  * Date: 2017-8-15
  * Time: 18:26
  * Description: 以编程方式，动态指定元数据，将RDD转换为DataFrame
  */
object RDD2DataFrameProgrammatically {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD2DataFrameProgrammatically").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val lines = sc.textFile("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\txt\\students.txt")

    // 第一步，构造元素为Row的普通RDD
    // Pattern: students: RDD[Row]
    val students = lines.map(line => Row(line.split(",")(0).toInt, line.split(",")(1), line.split(",")(2).toInt))

    // 第二步，编程方式动态构造元数据
    // Pattern: structType: StructType
    val structType = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)))

    // 第三步，进行RDD到DataFrame的转换
    val studentDF = sqlContext.createDataFrame(students, structType)

    // 使用df
    studentDF.registerTempTable("students")

    val teenagerDF = sqlContext.sql("select * from students where age<=18")

    val teenagerRDD = teenagerDF.rdd

    teenagerRDD.collect().foreach(row => println(row))

  }


}
