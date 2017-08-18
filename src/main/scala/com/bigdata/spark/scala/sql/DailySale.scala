package com.bigdata.spark.scala.sql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created with IntelliJ IDEA.
  * Author: CuiCan
  * Date: 2017-8-18
  * Time: 12:27
  * Description: 案例实战：根据每天的用户访问日志和用户购买日志，统计每日的uv和销售额
  * import sqlContext.implicits._
  * import org.apache.spark.sql.functions._
  */
object DailySale {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DailySale").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 要使用Spark SQL的内置函数， 就必须在这里导入SQLContext下的隐式转换
    // import sqlContext.implicits._
    import sqlContext.implicits._

    // 说明一下，业务的特点
    // 实际上呢，我们可以做一个，单独统计网站登录用户的销售额的统计
    // 有些时候，会出现日志的上报的错误和异常，比如日志里丢了用户的信息，那么这种，我们就一律不统计了

    // 模拟数据
    val userSaleLog = Array(
      "2015-10-01,55.05,1122",
      "2015-10-01,23.15,1133",
      "2015-10-01,15.20,",
      "2015-10-02,56.05,1144",
      "2015-10-02,78.87,1155",
      "2015-10-02,113.02,1123")
    val userSaleLogRDD = sc.parallelize(userSaleLog, 3)

    // 进行有效销售日志的过滤
    val filtereduserSaleLogRDD = userSaleLogRDD.filter(line => if (line.split(",").length == 3) true else false)

    // 转成Row RDD ，在将RDD转成DataFrame
    val userSaleLogRowRDD = filtereduserSaleLogRDD.map(line => Row(line.split(",")(0), line.split(",")(1).toDouble))

    val structType = StructType(Array(
      StructField("date", StringType, true),
      StructField("sale_amount", DoubleType, true)))
    val userSaleLogDF = sqlContext.createDataFrame(userSaleLogRowRDD, structType)

    userSaleLogDF.show()

    // 开始进行每日销售额的统计
    userSaleLogDF.groupBy("date")
      .agg('date, sum('sale_amount))
      .map(row => Row(row(1), row(2)))
      .collect()
      .foreach(println)

  }

}
