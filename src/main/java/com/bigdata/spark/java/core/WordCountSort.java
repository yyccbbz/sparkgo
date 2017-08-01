package com.bigdata.spark.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: CuiCan
 * @Date: 2017-7-24
 * @Time: 15:28
 * @Description: 排序的wordcount程序
 */
public class WordCountSort {

    public static void main(String[] args) {
        //编写spark应用程序
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("WordCountSort").setMaster("local"));

        JavaRDD<String> lines = sc.textFile("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\txt\\spark.txt");

        /**
         * 1、对文本文件内的每个单词都统计出其出现的次数。
         */
        JavaPairRDD<String, Integer> wordCounts = lines.flatMap(line -> Arrays.asList(line.split(" ")))
                .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
                .reduceByKey((v1, v2) -> v1 + v2);
        wordCounts.foreach(t -> System.out.println(t._1 + " - " + t._2));

        /**
         * 2、按照每个单词出现次数的数量，降序排序。
         *
         * key-value 反转、排序、反转
         *
         */
        JavaPairRDD<String, Integer> sortWordCounts = wordCounts.mapToPair(t -> new Tuple2<Integer, String>(t._2, t._1))
                            .sortByKey(false).mapToPair(t -> new Tuple2<>(t._2, t._1));
        sortWordCounts.foreach(t -> System.out.println(t._1 + " - " + t._2));
        sc.close();
    }

}
