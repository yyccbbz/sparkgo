package com.bigdata.spark.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: CuiCan
 * @Date: 2017-7-30
 * @Time: 17:22
 * @Description: action算子案例
 */
public class ActionOperator {

    public static void main(String[] args) {

        saveAsTextFile();
//        take();
//        count();
//        collect();
//        reduce();
    }

    /**
     * 1、reduce：将RDD中的所有元素进行聚合操作。第一个和第二个元素聚合，值与第三个元素聚合，值与第四个元素聚合，以此类推。
     * 2、collect：将RDD中所有元素获取到本地客户端。
     * 3、count：获取RDD元素总数。
     * 4、take：获取RDD中前n个元素。
     * 5、saveAsTextFile：将RDD元素保存到文件中，对每个元素调用toString方法
     * 6、countByKey：对每个key对应的值进行count计数
     * 7、foreach：遍历RDD中的每个元素
     */
    private static void reduce() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("reduce").setMaster("local"));
        List<Integer> numbersList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numbersList);

        Integer counts = numbers.reduce((v1, v2) -> v1 + v2);

        System.out.println("counts = " + counts);

        sc.close();
    }

    private static void collect() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("collect").setMaster("local"));
        List<Integer> numbersList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numbersList);

        JavaRDD<Integer> doubleNumRDD = numbers.map(x -> x * 2);
        /**
         * 这种方式一般不建议使用，数据从远程拉取到本地，性能较差
         * 在数据量特别大的时候，有可能OVM异常，内存溢出
         */
        List<Integer> list = doubleNumRDD.collect();

        for (Integer n : list) {
            System.out.println("n = " + n);
        }

        sc.close();
    }

    private static void count() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("count").setMaster("local"));
        List<Integer> numbersList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numbersList);

        /**
         * 统计RDD中元素的个数，极少使用
         */
        long count = numbers.count();
        System.out.println("count = " + count);

        sc.close();
    }

    private static void take() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("take").setMaster("local"));
        List<Integer> numbersList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numbersList);

        /**
         * 类似于collect（），也是从远程拉取数据到本地，
         * collect是拉取全部，take是拉取前几个
         */
        List<Integer> take = numbers.take(3);
        for (Integer n : take) {
            System.out.println("n = " + n);
        }
        sc.close();
    }

    private static void saveAsTextFile() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("saveAsTextFile"));
        List<Integer> numbersList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numbersList);

        JavaRDD<Integer> doubleNumRDD = numbers.map(x -> x * 2);
        /**
         * 直接把RDD保存在文件中
         * hdfs目录下三个文件：
         * /double_number.txt/_SUCCESS
         * /double_number.txt/part-00000
         * /double_number.txt/part-00001
         */
        doubleNumRDD.saveAsTextFile("hdfs://bd-cdh-master01:8020/double_number");

        sc.close();
    }

}
