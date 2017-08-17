package com.bigdata.spark.scala.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Author: CuiCan
  * Date: 2017-8-17
  * Time: 12:06
  * Description: JSON数据源  查询成绩为80分以上的学生的基本信息与成绩信息
  */
object JSONDataSource {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("JSONDataSource").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 创建学生成绩DataFrame
    val studentScoresDF = sqlContext.read.json("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\json\\students_score.json")

    // 查询出分数大于80分的学生成绩信息，以及学生姓名
    studentScoresDF.registerTempTable("student_scores")
    val goodStudentScoresDF = sqlContext.sql("select name,score from student_scores where score>=80")
    goodStudentScoresDF.show()
    println("=========好学生成绩信息表=========")

    val goodStudentNames = goodStudentScoresDF.rdd.map(row => row(0)).collect()

    // 创建学生基本信息DataFrame
    val studentInfoJSONs = Array("{\"name\":\"Leo\", \"age\":18}",
                                  "{\"name\":\"Marry\", \"age\":17}",
                                  "{\"name\":\"Jack\", \"age\":19}")
    /* studentInfoJSONsRDD: RDD[String]*/
    val studentInfoJSONsRDD = sc.parallelize(studentInfoJSONs, 3)
    /* studentInfosDF: DataFrame*/
    val studentInfosDF = sqlContext.read.json(studentInfoJSONsRDD)

    // 查询分数大于80分的学生的基本信息
    studentInfosDF.registerTempTable("student_infos")

    var sql = "select name,age from student_infos where name in("
    for (i <- 0 until goodStudentNames.length) {
      sql += "'" + goodStudentNames(i) + "'"
      if (i < goodStudentNames.length - 1) sql += ","
    }
    sql += ")"

    println(sql)

    val goodStudentInfosDF = sqlContext.sql(sql)
    goodStudentInfosDF.show()
    println("=========好学生基本信息表=========")

    // 将分数大于80分的学生的成绩信息与基本信息进行join
    val goodStudentsRDD = goodStudentScoresDF.rdd.map(row => (row.getAs[String]("name"), row.getAs[Long]("score")))
      .join(goodStudentInfosDF.rdd.map(row => (row.getAs[String]("name"), row.getAs[Long]("age"))))

    // 将rdd转换为dataframe
    val goodStudentRowsRDD = goodStudentsRDD.map(t => Row(t._1, t._2._1.toInt, t._2._1.toInt))

    val structType = StructType(Array(
      StructField("name", StringType, true),
      StructField("score", IntegerType, true),
      StructField("age", IntegerType, true)
    ))

    val goodStudentsDF = sqlContext.createDataFrame(goodStudentRowsRDD, structType)

    goodStudentsDF.printSchema()
    goodStudentsDF.show()
    println("=========好学生 基本+成绩 信息表=========")

    // 将dataframe中的数据保存到json中
    goodStudentsDF.write.format("json").save("./tmp/good-students-scala")


  }

}
