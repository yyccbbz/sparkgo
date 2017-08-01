package com.bigdata.spark.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: CuiCan
 * @Date: 2017-7-24
 * @Time: 15:28
 * @Description: 1、对文本文件内的数字，取最大的前3个。
 */
public class Top3 {

    public static void main(String[] args) {
        //编写spark应用程序
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("Top3").setMaster("local"));

        JavaRDD<String> lines = sc.textFile("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\txt\\top.txt");

        List<String> top3 = lines.mapToPair(line -> new Tuple2<String, String>(line, line))
                .sortByKey(false).map(t -> t._1).take(3);

        for (String s : top3) {
            System.out.println("s = " + s);
        }
        sc.close();
    }

}
