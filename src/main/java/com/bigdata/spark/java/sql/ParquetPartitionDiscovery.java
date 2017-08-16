package com.bigdata.spark.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Author: CuiCan
 * Date: 2017-8-16
 * Time: 14:08
 * Description: Parquet数据源之 自动推断分区
 */
public class ParquetPartitionDiscovery {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("ParquetPartitionDiscovery").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // 读取parquet文件中的数据，创建一个DataFrame
        DataFrame usersDF = sqlContext.read()
                .parquet("hdfs://devcluster/users/gender=male/country=US/users.parquet");

        usersDF.printSchema();
        /**
         * root
         *  |-- name: string (nullable = false)
         *  |-- favorite_color: string (nullable = true)
         *  |-- favorite_numbers: array (nullable = false)
         *  |    |-- element: integer (containsNull = false)
         *  |-- gender: string (nullable = true)
         *  |-- country: string (nullable = true)
         */

        usersDF.show();
        /**
         * +------+--------------+----------------+------+-------+
         * |  name|favorite_color|favorite_numbers|gender|country|
         * +------+--------------+----------------+------+-------+
         * |Alyssa|          null|  [3, 9, 15, 20]|  male|     US|
         * |   Ben|           red|              []|  male|     US|
         * +------+--------------+----------------+------+-------+
         */
    }

}
