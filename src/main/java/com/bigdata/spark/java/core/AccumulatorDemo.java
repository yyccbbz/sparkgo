package com.bigdata.spark.java.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: CuiCan
 * @Date: 2017-7-31
 * @Time: 15:08
 * @Description: 累加变量
 */
public class AccumulatorDemo {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("accumulatorDemo").setMaster("local"));

        //定义累加变量
        Accumulator<Integer> sum = sc.accumulator(0);

        List<Integer> numList = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> nums = sc.parallelize(numList);

        /**
         * 用累加变量对集合中的元素求和
         *
         */
        nums.foreach(n -> sum.add(n));
        System.out.println("累加后的值： " + sum.value());

        sc.close();
    }

}
