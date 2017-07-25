package com.bigdata.spark.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: CuiCan
 * @Date: 2017-7-24
 * @Time: 15:28
 * @Description: 使用本地文件创建RDD，统计文本文件字数
 */
public class LocalFileToRDD {

    public static void main(String[] args) {
        //编写spark应用程序
        SparkConf conf = new SparkConf()
                .setAppName("LocalFileToRDD")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\txt\\spark.txt");

        //统计文本文件内的字数
        JavaRDD<Integer> lineLength = lines.map(line -> line.length());

        Integer counts = lineLength.reduce((v1, v2) -> v1 + v2);

        System.out.println("文件总字数 counts = " + counts);

        sc.close();
    }

}
