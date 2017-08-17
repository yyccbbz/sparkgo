package com.bigdata.spark.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Author: CuiCan
 * Date: 2017-8-16
 * Time: 14:08
 * Description: Parquet数据源之 合并元数据
 */
public class ParquetMergeSchema {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("ParquetMergeSchema").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // 首先，手动创建一个DataFrame，作为学生的基本信息数据，并将其写入目录




        // 读取parquet文件中的数据，创建一个DataFrame
        DataFrame usersDF = sqlContext.read()
                .parquet("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\parquet\\users.parquet");

        // 将df注册为临时表，然后使用SQL查询需要的数据
        usersDF.show();
        usersDF.registerTempTable("user");

        DataFrame df = sqlContext.sql("select name from user");

        // 对查询出来的df，进行transformation操作，处理数据，然后打印出来
        List<String> userNames = df.javaRDD().map(row -> "Name:" + row.getString(0)).collect();
        for (String userName : userNames) {
            System.out.println("userName = " + userName);
        }

    }

}
