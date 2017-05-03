FROM dockerfile/ubuntu

ENV SPARK_VERSION 2.1.0
ENV HADOOP_VERSION 2.7


RUN \
	apt-get update && \
	apt-get install -y python python-dev python-pip python-setuptools build-essential git && \
	wget -q http://www.apache.org/dyn/closer.lua/spark/spark-"$SPARK_VERSION"/spark-"$SPARK_VERSION"-bin-hadoop"$HADOOP_VERSION".tgz -O /tmp/spark_"$SPARK_VERSION"-"$HADOOP_VERSION".tgz && \	
	tar xfz /tmp/spark_"$SPARK_VERSION"-"$HADOOP_VERSION".tgz -C /opt && \
    rm /tmp/spark_"$SPARK_VERSION"-"$HADOOP_VERSION".tgz && \
    echo -e "export SPARK_HOME=/opt/spark-"$SPARK_VERSION"-bin-hadoop"$HADOOP_VERSION"\nexport PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python\nexport PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python/lib/py4j-0.9-src.zip\nexport PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python/pyspark" >> ~/.bashrc && \
    cd $SPARK_HOME && \
    sudo easy_install pip && \
    sudo git clone https://github.com/mumrah/kafka-python && \
    sudo pip install kafka-python && \
    sudo pip install cassandra-driver && \
    cd $SPARK_HOME/lib && \
    wget http://central.maven.org/maven2/org/apache/spark/spark-streaming-kafka-assembly_2.11/1.6.3/spark-streaming-kafka-assembly_2.11-1.6.3.jar

COPY entry.sh /
COPY Main.py /
COPY StopWords.py /
RUN chmod +x /entry.sh

ENTRYPOINT ["/entry.sh"]

    







	
