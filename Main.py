from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from cassandra.cluster import Cluster
from datetime import datetime
from StopWords import stopWords
import sys
import json


'''Code by Gonzalo Bautista April-2017 '''

'''Lo que quiero obtener es:

    -numero de likes acumulados y por ventana de 10 min (tablas en cassandra: likesAccum y likesPerWindow)
    -numero de comentarios  acumulados y por ventana de 10 min(tablas en cassandra: commentsAccum commentsPerWindow)
    -Tener una actualizacion wordcount de las 10 palabras mas citadas en los comentarios en cada segmento
    de 10 minutos (tabla en cassandra: wordcountHist)

IMPORTANTE:

1:
entrar en la consola de cassandra y crear el keyspace:

cqlsh> create keyspace instagram WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

2: Instalar python cassandra-driver y kafka-python

sudo git clone https://github.com/mumrah/kafka-python
sudo pip install kafka-python
cd $SPARK_HOME/lib
wget http://central.maven.org/maven2/org/apache/spark/spark-streaming-kafka-assembly_2.10/1.6.2/spark-streaming-kafka-assembly_2.10-1.6.2.jar


'''

sc = SparkContext()
ssc = StreamingContext(sc, 5)

ssc.checkpoint("checkpoint")
#brokers = "localhost:9092"
brokers=sys.argv[1]
topiccomment = "comment"
topiclike = "like"

cassandra=sys.argv[2]

kvsComment = KafkaUtils.createDirectStream(ssc, [topiccomment], {"metadata.broker.list": brokers, "auto.offset.reset":"smallest"})
kvsLike = KafkaUtils.createDirectStream(ssc, [topiclike], {"metadata.broker.list": brokers,"auto.offset.reset":"smallest"})

def order (new_value, last_value):
    if last_value is None:
        last_value = 0
    return sum(new_value, last_value)

def saveCommentsAccum(x):
    def f(a):
        count=str(a[1])
        time=str(datetime.now())[0:15]
        cluster = Cluster([cassandra])
        session = cluster.connect('instagram')
        session.execute("create table if not exists commentsAccum (time bigint PRIMARY KEY, count counter)")
        updateStart="update commentsAccum set count=count+"
        updateEnd=" where time=toUnixTimestamp(now())"

        try:
            session.execute(updateStart+count+updateEnd)
        except:
            print "update not executed: ",count," ",time

        session.shutdown()
        cluster.shutdown()
    x.foreach(f)

def saveLikesAccum(x):
    def f(a):
        count=str(a[1])
        time=str(datetime.now())[0:15]
        cluster = Cluster([cassandra])
        session = cluster.connect('instagram')
        session.execute("create table if not exists likesAccum (time bigint PRIMARY KEY, count counter)")
        updateStart="update likesAccum set count=count+"
        updateEnd=" where time=toUnixTimestamp(now())"
        try:
            session.execute(updateStart+count+updateEnd)
        except:
            print "update not executed: ",count," ",time

        session.shutdown()
        cluster.shutdown()
    x.foreach(f)


def saveCommentsPerWindow(x):
    def f(a):
        count=str(a[1])
        time=str(datetime.now())[0:15]
        cluster = Cluster([cassandra])
        session = cluster.connect('instagram')
        session.execute("create table if not exists commentsPerWindow (time bigint PRIMARY KEY, count counter)")
        updateStart="update commentsPerWindow set count=count+"
        updateEnd=" where time=toUnixTimestamp(now())"

        try:
            session.execute(updateStart+count+updateEnd)
        except:
            print "update not executed: ",count," ",time
        session.shutdown()
        cluster.shutdown()
    x.foreach(f)

def saveLikesPerWindow(x):
    def f(a):
        count=str(a[1])
        time=str(datetime.now())[0:15]
        cluster = Cluster([cassandra])
        session = cluster.connect('instagram')
        session.execute("create table if not exists likesPerWindow (time bigint PRIMARY KEY, count counter)")
        updateStart="update likesPerWindow set count=count+"
        updateEnd=" where time=toUnixTimestamp(now())"
        try:
            session.execute(updateStart+count+updateEnd)
        except:
            print "update not executed: ",count," ",time
        session.shutdown()
        cluster.shutdown()
    x.foreach(f)


def saveWordCountInDb(x):
    cluster = Cluster([cassandra])
    session = cluster.connect('instagram')
    rdd=x.map(lambda x:(str(x[0]),x[1])).take(10)
    words=str(rdd).replace("'","")
    time = str(datetime.now())[0:15]
    session.execute("create table if not exists wordcountHist (time text primary key,words text)")
    startQuery = "select * from wordcountHist where time='"
    endQuery="'"
    try:
        query = session.execute(startQuery + time + endQuery).current_rows
    except:
        print "query not executed: ", str(x)
    if query == []:
        try:
            startInsert = "insert into wordcountHist (time,words) values ('"
            middleInsert = "','"
            endInsert = "')"
            session.execute(startInsert + time + middleInsert + words + endInsert)
        except:
            print "insert not executed: ", str(x)
    else:
        startDelete = "delete from wordcountHist where time='"
        endDelete = "'"
        session.execute(startDelete + time + endDelete)
        startInsert = "insert into wordcountHist (time,words) values ('"
        middleInsert = "','"
        session.execute(startInsert + time + middleInsert + words + '''')''')
    session.shutdown()
    cluster.shutdown()

commentAccum=kvsComment.map(lambda x: (x[0],1)).reduceByKey(lambda a,b:a+b).\
    updateStateByKey(order).foreachRDD(saveCommentsAccum)
likesAccum=kvsLike.map(lambda x: (x[0],1)).reduceByKey(lambda a,b:a+b). updateStateByKey(order)\
    .foreachRDD(saveLikesAccum)

commentPerWindow=kvsComment.map(lambda x: (x[0],1)).\
    reduceByKey(lambda a,b:a+b).foreachRDD(saveCommentsPerWindow)
likesPerWindow=kvsLike.map(lambda x: (x[0],1)).\
    reduceByKey(lambda a,b:a+b).foreachRDD(saveLikesPerWindow)



comments=kvsComment.map(lambda x:x[1]).map(lambda x: json.loads(x)['text'].encode('utf-8'))



commentWordCount=comments.flatMap(lambda x:x.split(' ')).\
    map(lambda x:x.replace('"text":','').replace('"',"").replace("'","").
    replace("(","").replace(")","").replace(",","").replace(".","")).\
    filter(lambda word: word not in stopWords).map(lambda x:(x,1)).\
    reduceByKey(lambda a,b:a+b).updateStateByKey(order).\
    transform(lambda rdd: rdd.sortBy(lambda (x,v): -v)).\
    foreachRDD(saveWordCountInDb)


ssc.start()

ssc.awaitTermination()