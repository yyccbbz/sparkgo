package com.bigdata.spark.scala.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created with IntelliJ IDEA.
  * Author: CuiCan
  * Date: 2017-8-16
  * Time: 14:32
  * Description: 
  */
object GenericLoadSave {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("GenericLoadSave").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.load("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\parquet\\users.parquet")

    df.printSchema()
    df.show()

    df.select("name", "favorite_color").write.save("./tmp/namesAndFavColors_scala.parquet")


  }

}
