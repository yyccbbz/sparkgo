#!/usr/bin/env bash
spark-submit \
--class com.bigdata.spark.java.core.WordCountCluster \
--num-executors 3 \
--driver-memory 1000m \
--executor-memory 1000m \
--executor-cores 3 \
./spark-go-1.0-SNAPSHOT.jar