package com.bigdata.spark.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: CuiCan
 * @Date: 2017-7-24
 * @Time: 15:28
 * @Description: 本地测试的wordcount程序
 */
public class WordCountLocal {

    public static void main(String[] args) {
        //编写spark应用程序
        SparkConf conf = new SparkConf()
                .setAppName("WordCountLocal")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\txt\\spark.txt");

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")));

        JavaPairRDD<String, Integer> counts = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1))
                                                   .reduceByKey((x, y) -> x + y);

        counts.foreach(count -> {
            System.out.println(count._1() + " : " + count._2());
        });

//        counts.saveAsTextFile("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\txt\\output.txt");

//        String inputFile = args[0];
//        String outputFile = args[1];
//        // Create a Java Spark Context.
//        SparkConf conf = new SparkConf().setAppName("wordCount");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        // Load our input data.
//        JavaRDD<String> input = sc.textFile(inputFile);
//        JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split(" ")));
//        JavaPairRDD<String, Integer> counts = words.mapToPair(x -> new Tuple2<String, Integer>(x, 1)).reduceByKey((x, y) -> x+y);
//        counts.saveAsTextFile(outputFile);
        sc.close();
    }

}
