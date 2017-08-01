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

        JavaPairRDD<String, Iterable<Integer>> pairRDD = lines.mapToPair(line ->
                new Tuple2<String, Integer>(line.split(" ")[0], Integer.parseInt(line.split(" ")[1])))
                .groupByKey();

        JavaPairRDD<String, Iterable<Integer>> top3 = pairRDD.mapToPair(t -> {
            Integer[] top3Arr = new Integer[3];
            Iterator<Integer> scoreIter = t._2.iterator();
            while (scoreIter.hasNext()) {
                Integer score = scoreIter.next();
                for (int i = 0; i < top3Arr.length; i++) {
                    if (top3Arr[i] == null) {
                        top3Arr[i] = score;
                        break;
                    } else if (score > top3Arr[i]) {
                        int tmp = top3Arr[i];
                        top3Arr[i] = score;
                        if (i < top3Arr.length - 1) {
                            top3Arr[i + 1] = tmp;
                        }
                    }
                }
            }
            return new Tuple2<String, Iterable<Integer>>(t._1, Arrays.asList(top3Arr));
        });

        top3.foreach(t -> {
            System.out.println("班级 " + t._1);
            Iterator<Integer> iter = t._2.iterator();
            while (iter.hasNext()) {
                System.out.println("分数 " + iter.next());
            }
            System.out.println("===========================");
        });

        sc.close();
    }

}
