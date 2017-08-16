package com.bigdata.spark.scala.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Author: CuiCan
  * Date: 2017-8-16
  * Time: 14:32
  * Description: 自动分区推断
  */
object ParquetPartitionDiscovery {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ParquetPartitionDiscovery").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val usersDF = sqlContext.read.
      load("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\parquet\\users\\gender=male\\country=US\\users.parquet")

    usersDF.printSchema()
    usersDF.show()

  }

}
