package com.bigdata.spark.scala.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Author: CuiCan
  * Date: 2017-8-16
  * Time: 14:32
  * Description: 编程方式加载parquet数据
  */
object ParquetLoadData {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ParquetLoadData").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val usersDF = sqlContext.read.load("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\parquet\\users.parquet")

    usersDF.printSchema()
    usersDF.show()

    // 注册临时表，处理df
    usersDF.registerTempTable("users")
    sqlContext.sql("select name from users").rdd
      .map(row => "Name: " + row.getAs[String]("name"))
      .collect()
      .foreach(name => println(name))

  }

}
