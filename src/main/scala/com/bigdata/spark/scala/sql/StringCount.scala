package com.bigdata.spark.scala.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


/**
  * Created with IntelliJ IDEA.
  * Author: CuiCan
  * Date: 2017-8-18
  * Time: 16:51
  * Description: 自定义UDAF函数, 统计字符串出现的次数
  */
class StringCount extends UserDefinedAggregateFunction {

  // inputSchema，指的是，输入数据的类型
  override def inputSchema: StructType =
    StructType(Array(StructField("str", StringType, true)))

  // bufferSchema，指的是，中间进行聚合时，所处理的数据的类型
  override def bufferSchema: StructType =
    StructType(Array(StructField("count", IntegerType, true)))

  // dataType，指的是，函数返回值的类型
  override def dataType: DataType = ???

  override def deterministic: Boolean = ???

  // 为每个分组的数据执行初始化操作
  override def initialize(buffer: MutableAggregationBuffer): Unit = ???

  // 指的是，每个分组，有新的值进来的时候，如何进行分组对应的聚合值的计算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???

  // 由于Spark是分布式的，所以一个分组的数据，可能会在不同的节点上进行局部聚合，就是update
  // 但是，最后一个分组，在各个节点上的聚合值，要进行merge，也就是合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

  // 最后，指的是，一个分组的聚合值，如何通过中间的缓存聚合值，最后返回一个最终的聚合值
  override def evaluate(buffer: Row): Any = ???
}
