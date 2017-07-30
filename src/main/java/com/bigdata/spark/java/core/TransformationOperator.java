package com.bigdata.spark.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
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
        //groupByKey();
        //reduceByKey();
        //sortByKey();
//        join();
        cogroup();
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
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("小一班", 89),
                new Tuple2<String, Integer>("小二班", 75),
                new Tuple2<String, Integer>("小二班", 94),
                new Tuple2<String, Integer>("小一班", 66)
        );
        JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoreList);

        /**
         * reduceByKey
         * 对每个key对应的value进行reduce操作。
         * 返回的依然是和源RDD类型一致
         */
        JavaPairRDD<String, Integer> javaPairRDD = scores.reduceByKey((x, y) -> x + y);
        javaPairRDD.foreach(t -> System.out.println("班级：" + t._1 + ", 总分数：" + t._2()));
        sc.close();
    }

    //6、sortByKey：将学生分数进行排序
    private static void sortByKey() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("sortByKey").setMaster("local"));
        List<Tuple2<Integer, String>> scoreList = Arrays.asList(
                new Tuple2<Integer, String>(89, "张三"),
                new Tuple2<Integer, String>(75, "李四"),
                new Tuple2<Integer, String>(94, "王五"),
                new Tuple2<Integer, String>(66, "赵六")
        );
        JavaPairRDD<Integer, String> scores = sc.parallelizePairs(scoreList);

        /**
         * sortByKey
         * 对每个key对应的value进行排序操作。
         * 返回的依然是和源RDD类型一致，元素内容还是保持不变，只是元素顺序不同
         * false 参数 为降序
         */
        JavaPairRDD<Integer, String> sortedRDD = scores.sortByKey(false);
        sortedRDD.foreach(t -> System.out.println(t._1 + " = " + t._2));

        sc.close();
    }

    //7、join：打印每个学生的成绩
    private static void join() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("join").setMaster("local"));

        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "张三"),
                new Tuple2<Integer, String>(2, "李四"),
                new Tuple2<Integer, String>(3, "王五"),
                new Tuple2<Integer, String>(4, "赵六")
        );
        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 60),
                new Tuple2<Integer, Integer>(4, 80)
        );

        JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);

        /**
         * join
         * 对两个包含<key,value>对的RDD进行join操作，
         * 每个key join上的pair，都会传入自定义函数进行处理。
         * 返回的JavaPairRDD类型，
         * 第一个泛型类型，是之前两个JavaPairRDD的key类型
         * 第二个泛型类型，是Tuple2<v1,v2>,
         * v1是第一个源RDD的value类型
         * v2是第二个源RDD的value类型
         */
        JavaPairRDD<Integer, Tuple2<String, Integer>> studentScores = students.join(scores);
        studentScores.foreach(t -> {
            System.out.println("序号:" + t._1 + " , 姓名:" + t._2._1 + ", 分数:" + t._2._2);
        });

        sc.close();
    }

    //8、cogroup：打印每个学生的成绩
    private static void cogroup() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("cogroup").setMaster("local"));
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "张三"),
                new Tuple2<Integer, String>(2, "李四"),
                new Tuple2<Integer, String>(3, "王五"),
                new Tuple2<Integer, String>(4, "赵六")
        );
        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 60),
                new Tuple2<Integer, Integer>(4, 80),
                new Tuple2<Integer, Integer>(1, 50),
                new Tuple2<Integer, Integer>(2, 40),
                new Tuple2<Integer, Integer>(3, 30),
                new Tuple2<Integer, Integer>(4, 20)
        );

        JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);

        /**
         * cogroup
         * 同join，但是是每个key对应的Iterable<value>都会传入自定义函数进行处理
         * 相当于一个key join上的所有value都放到一个Iterable里面去了
         */
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = students.cogroup(scores);

        cogroup.foreach(t -> {
            System.out.println("id = " + t._1);
            System.out.println("name = " + t._2._1);
            System.out.println("score = " + t._2._2);
//            System.out.println(t._1 + " " + t._2);
//            t._2._1.forEach(name -> System.out.println("name = " + name));
//            t._2._2.forEach(score -> System.out.println("score = " + score));
            System.out.println("=======================================");
        });
        sc.close();
    }

}
