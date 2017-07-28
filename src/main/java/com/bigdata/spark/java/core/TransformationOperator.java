package com.bigdata.spark.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: CuiCan
 * @Date: 2017-7-28
 * @Time: 15:30
 * @Description: transformation 算子实战
 */
public class TransformationOperator {

    public static void main(String[] args) {

        //map();
        //filter();
        //flatMap();
        groupByKey();
    }

    //1、map：将集合中每个元素乘以2
    private static void map() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("map").setMaster("local"));

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

        /**
         * map算子，对任何RDD都可以调用
         * 将RDD中的每个元素传入自定义函数，
         * 获取一个新的元素，然后用新的元素组成新的RDD
         */
        JavaRDD<Integer> multipleNumberRDD = numberRDD.map(n -> n * 2);

        multipleNumberRDD.foreach(v -> System.out.println("v = " + v));
        sc.close();
    }

    //2、filter：过滤出集合中的偶数
    private static void filter() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("filter").setMaster("local"));
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

        /**
         * filter算子，对任何RDD都可以调用
         * 对RDD中每个元素进行判断，
         * 如果返回true则保留，返回false则剔除。
         */
        JavaRDD<Integer> evenNumRDD = numberRDD.filter(x -> x % 2 == 0);

        evenNumRDD.foreach(v -> System.out.println("v = " + v));
        sc.close();
    }

    //3、flatMap：将行拆分为单词
    private static void flatMap() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("flatMap").setMaster("local"));
        List<String> stringList = Arrays.asList("hello java", "hello scala", "hello spark");
        JavaRDD<String> lines = sc.parallelize(stringList);

        /**
         * flatMap算子，与map类似，但是对每个元素都可以返回一个或多个新元素。
         * 接收原始RDD中的每个元素，并进行各种逻辑的计算和处理，返回多个元素，
         * 这些元素封装在Iterable集合中，可以使用ArrayList等集合接收并返回
         * 新的RDD大小一定>=原来的RDD大小
         */
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")));

        words.foreach(word -> System.out.println("word = " + word));
        sc.close();
    }

    //4、groupByKey：将每个班级的成绩进行分组
    private static void groupByKey() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("groupByKey").setMaster("local"));
        //模拟集合
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("小一班", 89),
                new Tuple2<String, Integer>("小二班", 75),
                new Tuple2<String, Integer>("小二班", 94),
                new Tuple2<String, Integer>("小一班", 66)
        );
        JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoreList);

        /**
         * groupByKey
         * 根据key进行分组，每个key对应一个Iterable<value>
         * 操作过返回的RDD，key类型和原RDD一致，value变成了一个Iterable<value>
         */
        JavaPairRDD<String, Iterable<Integer>> groupedScores = scores.groupByKey();

        /*groupedScores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println("class = " + t._1);
                Iterator<Integer> iterator = t._2.iterator();
                while (iterator.hasNext()){
                    System.out.println("score = " + iterator.next());
                }
                System.out.println("====================");
            }
        });*/

        groupedScores.foreach(t -> {
            System.out.println("班级 = " + t._1);
            t._2.forEach(s -> System.out.println("分数 = " + s));
            System.out.println("=====================");
        });

        sc.close();
    }

    //5、reduceByKey：统计每个班级的总分
    private static void reduceByKey() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("reduceByKey").setMaster("local"));


        sc.close();
    }

    //6、sortByKey：将学生分数进行排序
    private static void sortByKey() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("sortByKey").setMaster("local"));


        sc.close();
    }

    //7、join：打印每个学生的成绩
    private static void join() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("join").setMaster("local"));


        sc.close();
    }

    //8、cogroup：打印每个学生的成绩
    private static void cogroup() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("cogroup").setMaster("local"));


        sc.close();
    }

}
