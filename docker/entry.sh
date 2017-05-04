#! /bin/bash

cd $SPARK_HOME
bin/spark-submit --jars lib/spark-streaming-kafka-assembly_2.10-1.6.2.jar /InstaMetrics/Main.py $BROKER_KAFKA $CASSANDRA