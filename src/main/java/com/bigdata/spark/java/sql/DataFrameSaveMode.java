package com.bigdata.spark.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * Created with IntelliJ IDEA.
 * Author: CuiCan
 * Date: 2017-8-16
 * Time: 14:08
 * Description: Spark SQL对于save操作，提供了不同的save mode。
 */
public class DataFrameSaveMode {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("DataFrameSaveMode").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // 指定json格式
        DataFrame df = sqlContext.read().format("json")
                .load("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\json\\people.json");

        df.printSchema();
        df.show();

//        df.save("./tmp/people.json", SaveMode.ErrorIfExists);
        df.save("./tmp/people.json", SaveMode.Append);
//        df.save("./tmp/people.json", SaveMode.Overwrite);
//        df.save("./tmp/people.json", SaveMode.Ignore);

        /*df.select("name", "age").write().format("parquet")
                .save("./tmp/namesAndAges_java.parquet");*/

    }

}
