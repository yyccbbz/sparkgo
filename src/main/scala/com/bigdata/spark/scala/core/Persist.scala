package com.bigdata.spark.scala.core

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  *
  * Author: CuiCan
  * Date: 2017-7-31
  * Time: 16:14
  * Description: RDD持久化
  *              默认持久化策略：persist(StorageLevel.MEMORY_ONLY) = cache()
  */
object Persist {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LineCount").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\txt\\spark.txt")
                .persist(StorageLevel.MEMORY_ONLY)

    val startTime = System.currentTimeMillis()
    val count1 = rdd.count()
    val endTime = System.currentTimeMillis()
    println(count1)
    println("耗时一：" + (endTime - startTime) + " ms.")

    val beginTime = System.currentTimeMillis()
    val count2 = rdd.count()
    val overTime = System.currentTimeMillis()
    println(count2)
    println("耗时二：" + (overTime - beginTime) + " ms.")

  }
}
