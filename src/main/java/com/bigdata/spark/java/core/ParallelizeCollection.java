package com.bigdata.spark.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: CuiCan
 * @Date: 2017-7-25
 * @Time: 15:28
 * @Description: 并行化集合创建RDD,累加1到10
 */
public class ParallelizeCollection {

    public static void main(String[] args) {
        //创建SparkContext
        SparkConf conf = new SparkConf()
                .setAppName("ParallelizeCollection")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 并行化集合创建RDD
        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numRDD = sc.parallelize(nums,5);

        //执行reduce算子
        Integer counts = numRDD.reduce((x, y) -> x + y);

        System.out.println("counts = " + counts);

        sc.close();
    }

}
