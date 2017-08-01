package com.bigdata.spark.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created with IntelliJ IDEA.
 *
 * Author: CuiCan
 * Date: 2017-7-24
 * Time: 15:28
 * Description: 二次排序
 *      1，实现自定义的key，要实现Odered接口和Serializable接口，在key中实现对多个列的排序算法
 *      2，将包含文本的RDD，映射成key为自定义可以，value为文本的JavaPairRDD
 *      3，使用sortByKey算子按照自定义的key进行排序
 *      4，再次映射，剔除自定义的key，只保留文本行
 */
public class SecondarySort {

    public static void main(String[] args) {
        //编写spark应用程序
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("SecondarySort").setMaster("local"));

        JavaRDD<String> lines = sc.textFile("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\txt\\sort.txt");

        /**
         * 首先，把源RDD，变成 JavaPairRDD<SecondarySortKey, String> pairRDD 类型
         * 其次，按照自定义的规则进行排序
         * 再次，把排好序的RDD，去掉key，变成JavaRDD<String>类型
         * 得到最终排好序的结果
         */
        JavaPairRDD<SecondarySortKey, String> pairRDD = lines.mapToPair(line ->
                new Tuple2<SecondarySortKey, String>(
                        new SecondarySortKey(
                                Integer.parseInt(line.split(" ")[0]),
                                Integer.parseInt(line.split(" ")[1])), line
                ));

        JavaPairRDD<SecondarySortKey, String> sortedPairs = pairRDD.sortByKey();
        JavaRDD<String> sortedLines = sortedPairs.map(t -> t._2);

        sortedLines.foreach(s -> System.out.println("s = " + s));
        sc.close();
    }

}
