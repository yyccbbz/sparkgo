package com.bigdata.spark.scala.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Author: CuiCan
  * Date: 2017-8-16
  * Time: 14:32
  * Description: 
  */
object ManuallySpecifyOptions {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ManuallySpecifyOptions").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.format("json")
      .load("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\json\\people.json")

    df.printSchema()
    df.show()

    df.select("name", "age").write.format("parquet").save("./tmp/namesAndAges_scala.parquet")

  }

}
