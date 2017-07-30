#!/usr/bin/env bash
spark-submit \
--class com.bigdata.spark.java.core.WordCountCluster \
--num-executors 2 \
--driver-memory 100m \
--executor-memory 100m \
--executor-cores 1 \
./spark-go-1.0-SNAPSHOT.jar