spark-submit \
--class com.bigdata.spark.java.sql.DataFrameCreate \
--num-executors 3 \
--driver-memory 1000m \
--executor-memory 1000m \
--executor-cores 3 \
--files /usr/hdp/2.5.3.0-37/etc/hive/conf.dist/hive-site.xml \
--driver-class-path /usr/share/java/mysql-connector-java.jar \
spark-go-1.0-SNAPSHOT.jar
