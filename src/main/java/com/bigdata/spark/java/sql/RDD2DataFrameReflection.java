package com.bigdata.spark.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Author: CuiCan
 * Date: 2017-8-15
 * Time: 18:00
 * Description: 使用反射的方式将RDD转换为DataFrame
 */
public class RDD2DataFrameReflection {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("RDD2DataFrameReflection").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> lines = sc.textFile("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\txt\\students.txt");
        JavaRDD<Student> students = lines.map(line -> {
            String[] lineSplit = line.split(",");
            Student stu = new Student();
            stu.setId(Integer.valueOf(lineSplit[0].trim()));
            stu.setName(lineSplit[1]);
            stu.setAge(Integer.valueOf(lineSplit[2].trim()));
            return stu;
        });

        // 使用反射方式，将RDD转换为DataFrame
        DataFrame studentDF = sqlContext.createDataFrame(students, Student.class);
        studentDF.show();

        // 拿到一个df之后，就可以将其注册为一个临时表，然后针对其中的数据执行SQL语句
        studentDF.registerTempTable("students");

        // 执行sql语句
        DataFrame teenagerDF = sqlContext.sql("select * from students where age<=18");

        // 将查出来的Df，再次转换为RDD
        JavaRDD<Row> teenagerRDD = teenagerDF.javaRDD();

        // 将RDD中的数据，映射为Student，javabean必须序列化
        JavaRDD<Student> teenagerStudentRDD = teenagerRDD.map(row -> {
            Student stu = new Student();
            // 此时row中的顺序被打乱了
            stu.setAge(row.getInt(0));
            stu.setId(row.getInt(1));
            stu.setName(row.getString(2));
            return stu;
        });

        List<Student> studentList = teenagerStudentRDD.collect();
        for (Student stu : studentList) {
            System.out.println("stu = " + stu);
        }
    }

}
