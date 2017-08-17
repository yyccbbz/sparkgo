package com.bigdata.spark.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Author: CuiCan
 * Date: 2017-8-16
 * Time: 14:08
 * Description: JSON数据源
 * 查询成绩为80分以上的学生的基本信息与成绩信息
 */
public class JSONDataSource {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("JSONDataSource").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // 针对json文件，创建DataFrame
        DataFrame studentScoresDF = sqlContext.read()
                .json("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\json\\students_score.json");

        // 针对学生成绩信息的DataFrame，注册临时表，查询分数大于80分的学生的姓名
        studentScoresDF.printSchema();
        studentScoresDF.show();
        studentScoresDF.registerTempTable("student_scores");

        DataFrame goodStudentScoresDF = sqlContext.sql("select name,score from student_scores where score>=80");

        // 对查询出来的df先转为RDD，然后进行transformation操作，处理数据
        List<String> goodStudentNames = goodStudentScoresDF.javaRDD()
                .map(row -> row.getString(0)).collect();

        // 然后针对JavaRDD<String>，创建DataFrame （针对包含json串的JavaRDD，创建DataFrame）
        ArrayList<String> studentInfoJSONs = new ArrayList<>();
        studentInfoJSONs.add("{\"name\":\"Leo\", \"age\":18}");
        studentInfoJSONs.add("{\"name\":\"Marry\", \"age\":17}");
        studentInfoJSONs.add("{\"name\":\"Jack\", \"age\":19}");
        JavaRDD<String> studentInfoJSONsRDD = sc.parallelize(studentInfoJSONs);
        DataFrame studentInfoDF = sqlContext.read().json(studentInfoJSONsRDD);

        studentInfoDF.printSchema();
        studentInfoDF.show();

        //注册临时表
        studentInfoDF.registerTempTable("students_info");

        String sql = "select name,age from students_info where name in (";
        for (int i = 0; i < goodStudentNames.size(); i++) {
            sql += "'" + goodStudentNames.get(i) + "'";
            if (i < goodStudentNames.size() - 1) {
                sql += ",";
            }
        }
        sql += ")";

        DataFrame goodStudentInfosDF = sqlContext.sql(sql);

        // 然后将两份数据的DataFrame，转换为JavaPairRDD，执行join transformation
        // （将DataFrame转换为JavaRDD，再map为JavaPairRDD，然后进行join）
        JavaPairRDD<String, Tuple2<Integer, Integer>> goodStudentsRDD = goodStudentScoresDF.javaRDD()
                .mapToPair(row -> new Tuple2<String, Integer>(row.getString(0),
                        Integer.valueOf(String.valueOf(row.getLong(1)))))
                .join(goodStudentInfosDF.javaRDD().mapToPair(row -> new Tuple2<String, Integer>(row.getString(0),
                        Integer.valueOf(String.valueOf(row.getLong(1))))));

        // 然后将封装在RDD中的好学生的全部信息，转换为一个JavaRDD<Row>的格式
        // （将JavaRDD，转换为DataFrame）
        JavaRDD<Row> goodStudentRowsRDD = goodStudentsRDD.map(t -> RowFactory.create(t._1, t._2._1, t._2._2));

        // 创建一份元数据，将JavaRDD<Row>转换为DataFrame
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));

        StructType structType = DataTypes.createStructType(structFields);

        DataFrame goodStudentsDF = sqlContext.createDataFrame(goodStudentRowsRDD, structType);

        goodStudentsDF.printSchema();
        goodStudentsDF.show();

        // 将好学生的全部信息保存到一个json文件中去
        // （将DataFrame中的数据保存到外部的json文件中去）
        goodStudentsDF.write().format("json").save("./tmp/good-students");
    }

}
