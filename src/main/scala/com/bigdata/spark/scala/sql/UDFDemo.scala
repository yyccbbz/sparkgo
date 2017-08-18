package com.bigdata.spark.scala.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Created with IntelliJ IDEA.
  * Author: CuiCan
  * Date: 2017-8-18
  * Time: 16:40
  * Description: UDF：User Defined Function。用户自定义函数。
  *
  */
object UDFDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UDFDemo").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 构造模拟数据
    val names = Array("Leo", "Marry", "Jack", "Tom")
    val namesRDD = sc.parallelize(names, 3)
    val namesRowRDD = namesRDD.map(name => Row(name))
    val structType = StructType(Array(StructField("name", StringType, true)))
    val namesDF = sqlContext.createDataFrame(namesRowRDD, structType)

    namesDF.show()

    // 注册临时表
    namesDF.registerTempTable("names")

    // 定义和注册自定义函数
    // 定义函数：自己写匿名函数
    // 注册函数：SQLContext.udf.register()
    sqlContext.udf.register("strLen", (str: String) => str.length)

    // 使用自定义函数
    sqlContext.sql("select name,strLen(name) from names").collect().foreach(println)
  }

}
