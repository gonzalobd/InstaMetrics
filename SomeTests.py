from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Row
from cassandra.cluster import Cluster
import time

#from pyspark_cassandra import streaming


#import pyspark_cassandra
#import pyspark_cassandra.streaming

#from pyspark_cassandra import CassandraSparkContext


#spark = SparkSession.builder.getOrCreate()
#sc=spark.SparkContext

sc = SparkContext()
#sc = CassandraSparkContext()


ssc = StreamingContext(sc, 5)
ssc.checkpoint("checkpoint")
brokers = "localhost:9092"
topiccomment = "comment"
topiclike = "like"

kvsComment = KafkaUtils.createDirectStream(ssc, [topiccomment], {"metadata.broker.list": brokers})
kvsLike = KafkaUtils.createDirectStream(ssc, [topiclike], {"metadata.broker.list": brokers})


def order (new_value, last_value):
    if last_value is None:
        last_value = 0
    return sum(new_value, last_value)

def saveWordCountInDb(x):

    def f(x):
        cluster = Cluster()
        session = cluster.connect('instagram')
        word=str(x[0])
        count=str(x[1])
        startQuery="select * from wordcount where aux='x' and word='"
        endQuery="' allow filtering"
        try:
            query=session.execute(startQuery+word+endQuery).current_rows
        except:
            print "query not executed: ",str(x)
        if query==[]:
            try:
                startInsert="insert into wordcount (aux,word,count) values ('x','"
                middleInsert="',"
                endInsert=")"
                session.execute(startInsert+word+middleInsert+count+endInsert)
            except:
                print "insert not executed: ",word
        else:
            startDelete = "delete from wordcount where aux='x' and word='"
            endDelete="'"
            session.execute(startDelete + word +endDelete)
            startInsert = "insert into wordcount (aux,word,count) values ('x','"
            middleInsert = "',"
            session.execute(startInsert+word+middleInsert+count+''')''')
        session.shutdown()
    x.foreach(f)


def saveLikesPerWindow(x):

    def f(x,time):
        cluster = Cluster()
        session = cluster.connect('instagram')
        comienzo = "insert into likesPerWindow (time,count) values ('"
        mitad = "',"
        final = ")"
        try:
            session.execute(comienzo + float(time) + mitad + int(x[1]) + final)

        except:
            print "Query not executed: ", str(x)

        session.shutdown()
    timestamp=time.time()
    x.foreach(f,timestamp)

def saveCommentsPerWindow(x):

    def f(x):
        timestamp = time.time()
        cluster = Cluster()
        session = cluster.connect('instagram')
        comienzo = "insert into commentsPerWindow (time,count) values ("
        mitad = ","
        final = ")"
        try:
            session.execute(comienzo + str(timestamp) + mitad + str(x[1]) + final)
        except:
            print "Query not executed: ", str(x)

        session.shutdown()

    x.foreach(f)


comments=kvsComment.map(lambda x:x[1]).map(lambda x:x.split(','))\
    .map(lambda p: Row(created_time=p[0],
                                        media=p[1],
                                        text=p[2],
                                        username=p[3])).cache()




likes=kvsLike.map(lambda x: x[1]).map(lambda x: Row(id=x[0],
                                                    media=x[1],
                                                    username=x[2],
                                                    timestamp=x[3]))

likesPerWindow=kvsLike.map(lambda x: (x[0],1)).reduceByKey(lambda a,b:a+b)#.foreachRDD(saveLikesPerWindow)

commentPerWindow=kvsComment.map(lambda x: (x[0],1)).reduceByKey(lambda a,b:a+b)#.foreachRDD(saveCommentsPerWindow)

commentWordCount=comments.flatMap(lambda x:x.text.split(' ')).\
    map(lambda x:x.replace('"text":','').replace('"',"").replace("'","").replace("(","")
        .replace(")","").replace(",","")).map(lambda x:(x,1)).\
    reduceByKey(lambda a,b:a+b).updateStateByKey(order)#.\
    #foreachRDD(saveWordCountInDb)






#likePerWindow.pprint()
#commentPerWindow.pprint()
#commentWordCount.pprint()






ssc.start()


ssc.awaitTermination()