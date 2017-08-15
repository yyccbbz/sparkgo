package com.bigdata.spark.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created with IntelliJ IDEA.
 * Author: CuiCan
 * Date: 2017-8-15
 * Time: 16:58
 * Description: dataframe常见操作
 */
public class DataFrameOperation {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("DataFrameOperation");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // 创建出来的df，完全可以理解为一张表
        DataFrame df = sqlContext.read().json("hdfs://devcluster/students.json");

        //打印df中所有数据
        df.show();
        /**
         * +---+---+-----+
         * |age| id| name|
         * +---+---+-----+
         * | 18|  1|  leo|
         * | 19|  2| jack|
         * | 17|  3|marry|
         * +---+---+-----+
         */

        //打印df的元数据（scheme）
        df.printSchema();
        /**
         * root
         *  |-- age: long (nullable = true)
         *  |-- id: long (nullable = true)
         *  |-- name: string (nullable = true)
         */

        //查询某一列所有的数据
        df.select("name").show();
        /**
         * +-----+
         * | name|
         * +-----+
         * |  leo|
         * | jack|
         * |marry|
         * +-----+
         */

        //查询某几列所有的数据，并队列进行计算
        df.select(df.col("name"), df.col("age").plus(1)).show();
        /**
         * +-----+---------+
         * | name|(age + 1)|
         * +-----+---------+
         * |  leo|       19|
         * | jack|       20|
         * |marry|       18|
         * +-----+---------+
         */

        //根据某一列的值进行过滤
        df.filter(df.col("age").gt(18)).show();
        /**
         * +---+---+----+
         * |age| id|name|
         * +---+---+----+
         * | 19|  2|jack|
         * +---+---+----+
         */

        //根据某一列进行分组，然后进行组合
        df.groupBy("age").count().show();
        /**
         * +---+-----+
         * |age|count|
         * +---+-----+
         * | 17|    1|
         * | 18|    1|
         * | 19|    1|
         * +---+-----+
         */
    }

}
