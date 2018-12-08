from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark instance with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("OFF")
# setting a checkpoint to allow RDD recovery

logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.OFF)

ssc = StreamingContext(sc, 3)   #Streaming will execute in each 3 seconds

# split each tweet into words
dataStream = ssc.socketTextStream("localhost",9009)
counts = dataStream.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)

counts.pprint()
ssc.start()
ssc.awaitTermination()