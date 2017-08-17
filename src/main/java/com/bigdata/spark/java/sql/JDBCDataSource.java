package com.bigdata.spark.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Author: CuiCan
 * Date: 2017-8-16
 * Time: 14:08
 * Description: Hive数据源
 * 查询成绩为80分以上的学生的基本信息与成绩信息
 */
public class JDBCDataSource {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("JDBCDataSource").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        /**
         * 总结一下
         * jdbc数据源
         * 首先，是通过SQLContext的read系列方法，将mysql中的数据加载为DataFrame
         * 然后可以将DataFrame转换为RDD，使用Spark Core提供的各种算子进行操作
         * 最后可以将得到的数据结果，通过foreach()算子，写入mysql、hbase、redis等等db
         * cache中
         */

        // 分别将mysql中两张表的数据加载为DataFrame
        Map<String, String> options = new HashMap<String, String>();
        options.put("url", "jdbc:mysql://116.62.134.242:3306/testdb");
        options.put("dbtable", "student_infos");
        DataFrame studentInfosDF = sqlContext.read().format("jdbc").options(options).load();

        options.put("dbtable", "student_scores");
        DataFrame studentScoresDF = sqlContext.read().format("jdbc").options(options).load();

        // 将两个DataFrame转换为JavaPairRDD，执行join操作
        JavaPairRDD<String, Tuple2<Integer, Integer>> studentsRDD = studentInfosDF.javaRDD().mapToPair(row -> new Tuple2<String, Integer>(row.getString(0), row.getInt(1)))
                .join(studentScoresDF.javaRDD()
                        .mapToPair(row ->
                                new Tuple2<String, Integer>(row.getString(0), row.getInt(1))));


        // 将JavaPairRDD转换为JavaRDD<Row>
        JavaRDD<Row> studentRowsRDD = studentsRDD.map(t -> RowFactory.create(t._1, t._2._1, t._2._2));


        // 过滤出分数大于80分的数据
        JavaRDD<Row> filteredStudentRowsRDD = studentRowsRDD.filter(row -> {
            if (row.getInt(2) > 80) {
                return true;
            }
            return false;
        });

        // 转换为DataFrame
        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));

        StructType structType = DataTypes.createStructType(structFields);
        DataFrame studentsDF = sqlContext.createDataFrame(filteredStudentRowsRDD, structType);

        studentsDF.printSchema();
        studentsDF.show();

        Row[] rows = studentsDF.collect();
        for (Row row : rows) {
            System.out.println("row = " + row);
        }

        // 将DataFrame中的数据保存到mysql表中
        // 这种方式是在企业里很常用的，有可能是插入mysql、有可能是插入hbase，还有可能是插入redis缓存
        studentsDF.javaRDD().foreach(row ->{

        });
    }

}
