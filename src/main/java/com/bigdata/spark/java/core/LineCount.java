package com.bigdata.spark.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: CuiCan
 * @Date: 2017-7-28
 * @Time: 15:30
 * @Description: 统计每行出现的次数
 */
public class LineCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\txt\\hello.txt");

        JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(line -> new Tuple2<>(line, 1));

        JavaPairRDD<String, Integer> lineCounts = pairRDD.reduceByKey((x, y) -> x + y);

        lineCounts.foreach(t -> System.out.println(t._1 + " 出现次数为： " + t._2));

        sc.close();
    }

}
