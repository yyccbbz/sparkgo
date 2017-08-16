package com.bigdata.spark.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created with IntelliJ IDEA.
 * Author: CuiCan
 * Date: 2017-8-16
 * Time: 14:08
 * Description: 通用的load和save操作
 */
public class GenericLoadSave {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("GenericLoadSave").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame df = sqlContext.read().load("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\parquet\\users.parquet");

        df.printSchema();
        /**
         * root
         *  |-- name: string (nullable = false)
         *  |-- favorite_color: string (nullable = true)
         *  |-- favorite_numbers: array (nullable = false)
         *  |    |-- element: integer (containsNull = false)
         */
        df.show();
        /**
         * +------+--------------+----------------+
         * |  name|favorite_color|favorite_numbers|
         * +------+--------------+----------------+
         * |Alyssa|          null|  [3, 9, 15, 20]|
         * |   Ben|           red|              []|
         * +------+--------------+----------------+
         */

        df.select("name", "favorite_color")
                .write().save("./tmp/namesAndFavColors_java.parquet");

    }

}
