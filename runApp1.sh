#!/bin/bash

source ~/run.sh
hadoop fs -rmr /tmp/spark_ranks.csv
./sbt.sh package
spark-submit --class "PageRankSpark" --master spark://10.254.0.177:7077 ./target/scala-2.11/pagerankspark_2.11-1.0.jar
