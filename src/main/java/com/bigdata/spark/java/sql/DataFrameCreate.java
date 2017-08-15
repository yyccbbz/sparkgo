package com.bigdata.spark.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created with IntelliJ IDEA.
 * Author: CuiCan
 * Date: 2017-8-15
 * Time: 16:23
 * Description: 使用json文件 创建dataframe
 */
public class DataFrameCreate {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("DataFrameCreate");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        DataFrame df = sqlContext.read().json("hdfs://devcluster/students.json");

        df.show();
    }
}
