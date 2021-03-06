package com.bigdata.spark.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: CuiCan
 * @Date: 2017-7-31
 * @Time: 15:08
 * @Description: 广播变量
 */
public class BroadcastVariable {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("BroadcastVariable").setMaster("local"));

        final int factor = 3;
        //定义广播变量
        final Broadcast<Integer> factorBroadcast = sc.broadcast(factor);

        List<Integer> numList = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> nums = sc.parallelize(numList);

        /**
         * 让集合中每个元素都乘以factor
         *
         */
        JavaRDD<Integer> newNums = nums.map(n -> n * factorBroadcast.value());
        newNums.foreach(n -> System.out.println("n = " + n));

        sc.close();
    }

}
