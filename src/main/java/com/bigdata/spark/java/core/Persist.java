package com.bigdata.spark.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: CuiCan
 * @Date: 2017-7-31
 * @Time: 15:08
 * @Description: RDD持久化
 */
public class Persist {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("persist").setMaster("local"));

        /**
         * cache()或者persist()的使用，是有规则的
         * 必须在transformation或者textFile等创建一个RDD之后，直接连续调用才可以。
         * 如果先创建RDD，再另起一行调用方法，会报错，大量的文件会丢失。
         */
        JavaRDD<String> lines = sc.textFile("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\txt\\spark.txt")
                .cache();

        long beginTime = System.currentTimeMillis();
        long count = lines.count();
        long endTime = System.currentTimeMillis();
        System.out.println("count = " + count);
        System.out.println("第一次耗时：" + (endTime - beginTime)+" ms.");

        beginTime = System.currentTimeMillis();
        count = lines.count();
        endTime = System.currentTimeMillis();
        System.out.println("count = " + count);
        System.out.println("第二次耗时：" + (endTime - beginTime)+" ms.");

        sc.close();
    }

}
