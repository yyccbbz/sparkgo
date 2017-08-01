package com.bigdata.spark.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: CuiCan
 * @Date: 2017-7-24
 * @Time: 15:28
 * @Description: 2、对每个班级内的学生成绩，取出前3名。（分组取topn）
 */
public class GroupTop3 {

    public static void main(String[] args) {
        //编写spark应用程序
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setAppName("Top3").setMaster("local"));

//        JavaRDD<String> lines = sc.textFile("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\txt\\score.txt");
        JavaRDD<String> lines = sc.textFile("E:\\Users\\IdeaProjects\\sparkgo\\src\\main\\resources\\txt\\score.txt");

        JavaPairRDD<String, Iterable<Integer>> groupPairs = lines.mapToPair(line ->
                new Tuple2<String, Integer>(line.split(" ")[0], Integer.parseInt(line.split(" ")[1])))
                .groupByKey();

        JavaPairRDD<String, Iterable<Integer>> top3Pairs = groupPairs.mapToPair(t -> {
            Integer[] top3 = new Integer[3];
            String key = t._1();
            Iterator<Integer> values = t._2.iterator();
            while (values.hasNext()) {
                Integer value = values.next();

                for (int i = 0; i < top3.length; i++) {
                    if (top3[i] == null) {
                        top3[i] = value;
                        break;
                    } else if (value > top3[i]) { //value比索引处的值大
                        // 该位置之后的值向后移动
                        for (int j = 2; j > i; j--) {
                            top3[j] = top3[j - 1];
                        }
                        top3[i] = value;
                        break;
                    }
                    //否则， //索引处的值比value小， 在top5总继续向后比较
                }
            }
            return new Tuple2<String, Iterable<Integer>>(key, Arrays.asList(top3));
        });

        top3Pairs.foreach(t -> {
            System.out.print(t._1);
            Iterator<Integer> iter = t._2.iterator();
            while (iter.hasNext()) {
                System.out.print("\t" + iter.next());
            }
            System.out.println();
        });

        sc.close();
    }

}
