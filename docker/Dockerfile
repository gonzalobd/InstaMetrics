FROM dockerfile/ubuntu

ENV SPARK_VERSION 2.1.0
ENV HADOOP_VERSION 2.7


RUN \
	apt-get update && \
	apt-get install -y python python-dev python-pip && \
	wget -q http://www.apache.org/dyn/closer.lua/spark/spark-"$SPARK_VERSION"/spark-"$SPARK_VERSION"-bin-hadoop"$HADOOP_VERSION".tgz -O /tmp/spark_"$SPARK_VERSION"-"$HADOOP_VERSION".tgz && \	
	
