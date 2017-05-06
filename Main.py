from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.sql import Row
from cassandra.cluster import Cluster
from datetime import datetime
from StopWords import stopWords
import sys


'''Code by Gonzalo Bautista April-2017 '''

'''Lo que quiero obtener es:

    -numero de likes acumulados y por ventana de 10 min (tablas en cassandra: likesAccum y likesPerWindow)
    -numero de comentarios  acumulados y por ventana de 10 min(tablas en cassandra: commentsAccum commentsPerWindow)
    -Tener una actualizacion wordcount de las 10 palabras mas citadas en los comentarios en cada segmento
    de 10 minutos (tabla en cassandra: wordcountHist)
    -Saber cual es mi maximo historico de comentarios y likes y en que segmento de 10 minutos fue
    (tablas maxLikes y maxComents)

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
        session.execute("create table if not exists maxComments (max int PRIMARY KEY, time text)")
        updateStart="update commentsAccum set count=count+"
        updateEnd=" where time=toUnixTimestamp(now())"

        try:
            session.execute(updateStart+count+updateEnd)
        except:
            print "update not executed: ",count," ",time

        session.shutdown()
    x.foreach(f)

def saveLikesAccum(x):
    def f(a):
        count=str(a[1])
        time=str(datetime.now())[0:15]
        cluster = Cluster([cassandra])
        session = cluster.connect('instagram')
        session.execute("create table if not exists likesAccum (time bigint PRIMARY KEY, count counter)")
        session.execute("create table if not exists maxLikes (max int PRIMARY KEY, time text)")
        updateStart="update likesAccum set count=count+"
        updateEnd=" where time=toUnixTimestamp(now())"
        try:
            session.execute(updateStart+count+updateEnd)
        except:
            print "update not executed: ",count," ",time

        session.shutdown()
    x.foreach(f)


def saveCommentsPerWindow(x):
    def f(a):
        count=str(a[1])
        time=str(datetime.now())[0:15]
        cluster = Cluster([cassandra])
        session = cluster.connect('instagram')
        session.execute("create table if not exists commentsPerWindow (time bigint PRIMARY KEY, count counter)")
        session.execute("create table if not exists maxComments (max int PRIMARY KEY, time text)")
        updateStart="update commentsPerWindow set count=count+"
        updateEnd=" where time=toUnixTimestamp(now())"

        try:
            session.execute(updateStart+count+updateEnd)
        except:
            print "update not executed: ",count," ",time

        '''como en cassandra no puedo obtener maximos segun tengo
        configurada la tabla, me creo una tabla donde
        guardare cuando tuve mi maximo historico'''

        query=session.execute("select * from maxComments").current_rows
        if query==[]:
            insertStart="insert into maxComments (max,time) values (0,'"
            insertEnd="')"
            try:
                session.execute(insertStart+time+insertEnd)
            except:
                print "insert not executed: ",insertStart,time,insertEnd
        else:
            currentMax=query[0].max
            if int(count)>int(currentMax):
                session.execute("truncate table maxComments")
                insertStart = "insert into maxComments (max,time) values ("
                insertMiddle = ",'"
                insertEnd = "')"
                try:
                    session.execute(insertStart + count + insertMiddle + time + insertEnd)
                except:
                    print "insert not executed: ",insertStart,count,insertMiddle,time,insertEnd
        session.shutdown()
    x.foreach(f)

def saveLikesPerWindow(x):
    def f(a):
        count=str(a[1])
        time=str(datetime.now())[0:15]
        cluster = Cluster([cassandra])
        session = cluster.connect('instagram')
        session.execute("create table if not exists likesPerWindow (time bigint PRIMARY KEY, count counter)")
        session.execute("create table if not exists maxLikes (max int PRIMARY KEY, time text)")
        updateStart="update likesPerWindow set count=count+"
        updateEnd=" where time=toUnixTimestamp(now())"
        try:
            session.execute(updateStart+count+updateEnd)
        except:
            print "update not executed: ",count," ",time

        '''como en cassandra no puedo obtener maximos segun tengo
                configurada la tabla, me creo una tabla donde
                guardare cuando tuve mi maximo historico'''

        query = session.execute("select * from maxLikes").current_rows
        if query == []:
            insertStart = "insert into maxLikes (max,time) values (0,'"
            insertEnd = "')"
            try:
                session.execute(insertStart + time + insertEnd)
            except:
                print "insert not executed: ", insertStart, count, time, insertEnd
        else:
            currentMax = query[0].max
            if int(count) > int(currentMax):
                session.execute("truncate table maxLikes")
                insertStart = "insert into maxLikes (max,time) values ("
                insertMiddle = ",'"
                insertEnd = "')"
                try:
                    session.execute(insertStart + count + insertMiddle + time + insertEnd)
                except:
                    print "insert not executed: ", insertStart, count, insertMiddle, time, insertEnd
        session.shutdown()
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

commentAccum=kvsComment.map(lambda x: (x[0],1)).reduceByKey(lambda a,b:a+b).\
    updateStateByKey(order).foreachRDD(saveCommentsAccum)
likesAccum=kvsLike.map(lambda x: (x[0],1)).reduceByKey(lambda a,b:a+b). updateStateByKey(order)\
    .foreachRDD(saveLikesAccum)

commentPerWindow=kvsComment.map(lambda x: (x[0],1)).reduceByKey(lambda a,b:a+b).foreachRDD(saveCommentsPerWindow)
likesPerWindow=kvsLike.map(lambda x: (x[0],1)).reduceByKey(lambda a,b:a+b).foreachRDD(saveLikesPerWindow)



comments=kvsComment.map(lambda x:x[1].encode('utf-8')).map(lambda x:x.split(','))\
    .map(lambda p: Row(text=p[0],
                                        username=p[1],
                                        created_time=p[2],
                                        media=p[3]))



commentWordCount=comments.flatMap(lambda x:x.text.split(' ')).\
    map(lambda x:x.replace('"text":','').replace('"',"").replace("'","").replace("(","")
        .replace(")","").replace(",","").replace(".","")).\
    filter(lambda word: word not in stopWords).map(lambda x:(x,1)).\
    reduceByKey(lambda a,b:a+b).updateStateByKey(order).\
    transform(lambda rdd: rdd.sortBy(lambda (x,v): -v)).\
    foreachRDD(saveWordCountInDb)


ssc.start()

ssc.awaitTermination()