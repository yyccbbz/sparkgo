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
 * Description: 手动指定数据源类型
 */
public class ManuallySpecifyOptions {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("ManuallySpecifyOptions").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // 指定json格式
        DataFrame df = sqlContext.read().format("json")
                .load("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\json\\people.json");

        df.printSchema();
        /**
         * root
         *  |-- age: long (nullable = true)
         *  |-- name: string (nullable = true)
         */

        df.show();
        /**
         * +----+-------+
         * | age|   name|
         * +----+-------+
         * |null|Michael|
         * |  30|   Andy|
         * |  19| Justin|
         * +----+-------+
         */

        df.select("name", "age").write().format("parquet")
                .save("./tmp/namesAndAges_java.parquet");

    }

}
