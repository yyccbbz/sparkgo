package com.bigdata.spark.scala.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Author: CuiCan
  * Date: 2017-8-15
  * Time: 16:43
  * Description: 创建dataframe
  */
object DataFrameCreate {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrameCreate")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("hdfs://devcluster/students.json")
    df.show()
  }

}
