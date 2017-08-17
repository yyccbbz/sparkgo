package com.bigdata.spark.scala.sql

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Author: CuiCan
  * Date: 2017-8-16
  * Time: 14:32
  * Description: parquet 合并元数据
  */
object ParquetMergeSchema {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ParquetMergeSchema").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    // 首先，创建一个DataFrame，作为学生的基本信息数据，并将其写入parquet文件
    val studentsWithNameAge = Array(("leo", 23), ("jack", 17))
    val studentsWithNameAgeDF = sc.parallelize(studentsWithNameAge).toDF("name", "age")
    studentsWithNameAgeDF.save("./tmp/students_scala", "parquet", SaveMode.Append)

    // 其次，创建另一个DataFrame，作为学生的成绩信息，并将其写入parquet文件
    val studentsWithNameGrade = Array(("marry", "A"), ("tom", "B"))
    val studentsWithNameGradeDF = sc.parallelize(studentsWithNameGrade).toDF("name", "grade")
    studentsWithNameGradeDF.save("./tmp/students_scala", "parquet", SaveMode.Append)

    // 第一个和第二个DataFrame的元数据，肯定是不一样的，需要变成三个列的合并
    // Pattern: students: DataFrame
    val students = sqlContext.read.option("mergeSchema", "true")
      .parquet("./tmp/students_scala")

    students.printSchema()
    /**
      * root
      *  |-- name: string (nullable = true)
      *  |-- age: integer (nullable = true)
      *  |-- grade: string (nullable = true)
      */
    students.show()
    /**
      * +-----+----+-----+
      * | name| age|grade|
      * +-----+----+-----+
      * |  leo|  23| null|
      * | jack|  17| null|
      * |marry|null|    A|
      * |  tom|null|    B|
      * +-----+----+-----+
      */


    // 注册临时表，处理数据
    students.registerTempTable("students")

    //TODO


  }

}
