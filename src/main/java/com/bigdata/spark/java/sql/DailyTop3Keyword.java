package com.bigdata.spark.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * Author: CuiCan
 * Date: 2017-8-22
 * Time: 10:40
 * Description: 每日top3热点搜索词统计
 * <p>
 * 数据格式：
 * 日期 用户 搜索词 城市 平台 版本
 * <p>
 * 需求：
 * 1、筛选出符合查询条件（城市、平台、版本）的数据
 * 2、统计出每天搜索uv排名前3的搜索词
 * 3、按照每天的top3搜索词的uv搜索总次数，倒序排序
 * 4、将数据保存到hive表中
 */
public class DailyTop3Keyword {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("DailyTop3Keyword").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc.sc());

        // 伪造出一份数据，查询条件
        // 备注：实际上，在实际的企业项目开发中，很可能，这个查询条件，是通过J2EE平台插入到某个MySQL表中的
        // 然后，这里呢，实际上，通常是会用Spring框架和ORM框架（MyBatis）的，去提取MySQL表中的查询条件
        Map<String, List<String>> queryParamMap = new HashMap<String, List<String>>();
        queryParamMap.put("city", Arrays.asList("shanghai"));
        queryParamMap.put("platform", Arrays.asList("Android"));
        queryParamMap.put("version", Arrays.asList("1.0", "2.0", "3.0", "5.0"));

        // 广播变量
        final Broadcast<Map<String, List<String>>> PARAM = sc.broadcast(queryParamMap);

        // 1、针对原始数据（HDFS文件），获取输入的RDD
        JavaRDD<String> lines = sc.textFile("E:\\Workspace\\intellij2017\\" +
                "sparkgo\\src\\main\\resources\\txt\\keyword.txt", 5);


        // 2、使用filter算子，去针对输入RDD中的数据，进行数据过滤，过滤出符合查询条件的数据。
        // 2.1 普通的做法：直接在fitler算子函数中，使用外部的查询条件（Map），
        // 但是，这样做的话，是不是查询条件Map，会发送到每一个task上一份副本。（性能并不好）

        // 2.2 优化后的做法：将查询条件，封装为Broadcast广播变量，
        // 在filter算子中使用Broadcast广播变量进行数据筛选。
        JavaRDD<String> filterRDD = lines.filter(line -> {

            String[] logSplited = line.split("\t");
            String city = logSplited[3];
            String platform = logSplited[4];
            String version = logSplited[5];

            // 与查询条件进行比对
            Map<String, List<String>> queryParams = PARAM.value();

            List<String> cities = queryParams.get("city");
            if (!cities.contains(city) && cities.size() > 0) {
                return false;
            }
            List<String> platforms = queryParams.get("platform");
            if (!platforms.contains(platform) && platforms.size() > 0) {
                return false;
            }
            List<String> versions = queryParams.get("version");
            if (!versions.contains(version) && versions.size() > 0) {
                return false;
            }

            return true;
        });

        // 3、将数据转换为“(日期_搜索词, 用户)”格式，然后呢，对它进行分组，
        // 然后再次进行映射，对每天每个搜索词的搜索用户进行去重操作，并统计去重后的数量，
        // 即为每天每个搜索词的uv。最后，获得“(日期_搜索词, uv)”
        JavaPairRDD<String, String> dateKeywordUserRDD = filterRDD.mapToPair(log -> {
            String[] logSplited = log.split("\t");
            String date = logSplited[0];
            String user = logSplited[1];
            String keyword = logSplited[2];

            return new Tuple2<String, String>(date + "_" + keyword, user);
        });

        // 4、将得到的每天每个搜索词的uv，RDD，映射为元素类型为Row的RDD，将该RDD转换为DataFrame
        // 分组，去重，获得uv
        JavaPairRDD<String, Long> dateKeywordUvRDD = dateKeywordUserRDD.groupByKey().mapToPair(t -> {
            String dateKeyword = t._1;
            Iterator<String> users = t._2.iterator();
            // 对用户进行去重，并统计去重后的数量
            List<String> distinctUsers = new ArrayList<String>();

            while (users.hasNext()) {
                String user = users.next();
                if (!distinctUsers.contains(user)) {
                    distinctUsers.add(user);
                }
            }

            // 获取uv
            long uv = distinctUsers.size();
            return new Tuple2<String, Long>(dateKeyword, uv);
        });

        JavaRDD<Row> dateKeywordUvRowRDD = dateKeywordUvRDD.map(t -> {
            String date = t._1.split("_")[0];
            String keyword = t._1.split("_")[1];
            long uv = t._2;

            return RowFactory.create(date, keyword, uv);
        });

        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("keyword", DataTypes.StringType, true),
                DataTypes.createStructField("uv", DataTypes.LongType, true));
        StructType structType = DataTypes.createStructType(structFields);
        DataFrame dateKeywordUvRowDF = hiveContext.createDataFrame(dateKeywordUvRowRDD, structType);

        dateKeywordUvRowDF.show();
        dateKeywordUvRowDF.printSchema();

        // 5、将DataFrame注册为临时表，使用Spark SQL的开窗函数，
        // 来统计每天的uv数量排名前3的搜索词，以及它的搜索uv，最后获取，是一个DataFrame
        dateKeywordUvRowDF.registerTempTable("daily_keyword_uv");

        DataFrame dailyTop3KeywordDF = hiveContext.sql("SELECT date,keyword,uv FROM (" +
                "SELECT date,keyword,uv," +
                "row_number() OVER (PARTITION BY date ORDER BY uv DESC) rank " +
                "FROM daily_keyword_uv ) tmp WHERE rank<=3");

        // 6、将DataFrame转换为RDD，继续操作，按照每天日期来进行分组，
        // 并进行映射，计算出每天的top3搜索词的搜索uv的总数，然后将uv总数作为key，
        // 将每天的top3搜索词以及搜索次数，拼接为一个字符串
        JavaPairRDD<String, String> top3DateKeywordUvRDD = dailyTop3KeywordDF.javaRDD().mapToPair(row -> {
            String date = row.getString(0);
            String keyword = row.getString(1);
            long uv = row.getLong(2);

            return new Tuple2<String, String>(date, keyword + "_" + uv);
        });

        JavaPairRDD<Long, String> uvDateKeywordsRDD = top3DateKeywordUvRDD.groupByKey().mapToPair(t -> {
            String date = t._1;

            Long totolUv = 0L;
            String dateKeyWords = date;

            Iterator<String> keywordUvIterator = t._2.iterator();
            while (keywordUvIterator.hasNext()) {
                String keywordUv = keywordUvIterator.next();
                Long uv = Long.valueOf(keywordUv.split("_")[1]);
                totolUv += uv;
                dateKeyWords += "," + keywordUv;
            }

            return new Tuple2<Long, String>(totolUv, dateKeyWords);
        });


        // 7、按照每天的top3搜索总uv，进行排序，倒序排序
        JavaPairRDD<Long, String> sortedUvDateKeywordsRDD = uvDateKeywordsRDD.sortByKey(false);

        // 8、将排好序的数据，映射回原始的格式，Iterable<Row>，变成“日期_搜索词_uv”的格式
        JavaRDD<Row> sortedRowRDD = sortedUvDateKeywordsRDD.flatMap(t -> {
            String dateKeywords = t._2;
            String[] dateKeywordsSplited = dateKeywords.split(",");

            String date = dateKeywordsSplited[0];
            List<Row> rows = new ArrayList<Row>();
            rows.add(RowFactory.create(date,
                    dateKeywordsSplited[1].split("_")[0],
                    Long.valueOf(dateKeywordsSplited[1].split("_")[1])));
            rows.add(RowFactory.create(date,
                    dateKeywordsSplited[2].split("_")[0],
                    Long.valueOf(dateKeywordsSplited[2].split("_")[1])));
            rows.add(RowFactory.create(date,
                    dateKeywordsSplited[3].split("_")[0],
                    Long.valueOf(dateKeywordsSplited[3].split("_")[1])));

            return rows;
        });

        // 9、再次映射为DataFrame，并将数据保存到Hive中即可
        DataFrame df = hiveContext.createDataFrame(sortedRowRDD, structType);

//        df.saveAsTable("daily_top3_keyword_uv");

        df.show();

        sc.close();
    }
}
