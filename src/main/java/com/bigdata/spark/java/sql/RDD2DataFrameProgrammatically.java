package com.bigdata.spark.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Author: CuiCan
 * Date: 2017-8-15
 * Time: 18:00
 * Description: 以编程方式，动态指定元数据，将RDD转换为DataFrame
 */
public class RDD2DataFrameProgrammatically {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("RDD2DataFrameProgrammatically").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // 创建一个普通的RDD，但是必须将其转换成RDD<Row>的这种格式
        JavaRDD<String> lines = sc.textFile("E:\\Workspace\\intellij2017\\sparkgo\\src\\main\\resources\\txt\\students.txt");

        // 类型转换问题，row中塞数据，注意数据格式
        JavaRDD<Row> studentRDD = lines.map(line -> {
            String[] lineSplit = line.split(",");
            return RowFactory.create(Integer.valueOf(lineSplit[0]), lineSplit[1], Integer.valueOf(lineSplit[2]));
        });

        // 动态构造元数据，例如，id、name、age等，field的名称和类型，可能都是在程序运行过程中，动态从mysql db中
        // 或者是配置文件中，加载出来的，是不固定的，所以特别适合使用变成方式来构造元数据
        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        // 使用动态构造的元数据，将RDD转换为DataFrame
        DataFrame studentDF = sqlContext.createDataFrame(studentRDD, structType);

        // 后面就可以使用df
        studentDF.registerTempTable("students");

        DataFrame teenagerDF = sqlContext.sql("select * from students where age<=18");

        List<Row> rows = teenagerDF.javaRDD().collect();
        for (Row row : rows) {
            System.out.println("row = " + row);
        }

    }

}
