
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkConf
import json

if __name__ == "__main__":
    # Create Spark Context to Connect Spark Cluster
    mapLocal = {}
    conf = SparkConf()
    conf.set('spark.logConf', 'true')
    sc = SparkContext(appName="PythonStreamingKafkaTweetCount")
    sc.setLogLevel("ERROR")
    # Set the Batch Interval is 10 sec of Streaming Context
    ssc = StreamingContext(sc, 10)

    # Create Kafka Stream to Consume Data Comes From Twitter Topic
    # localhost:2181 = Default Zookeeper Consumer Address
    kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'tweets-lambda1': 1})

    # Parse Twitter Data as json
    parsed = kafkaStream.map(lambda v: json.loads(v[1]))

    # Count the number of tweets per User
    user_counts = parsed.map(lambda tweet: (tweet['user']["screen_name"], 1)).reduceByKey(lambda x, y: x + y)

    # Print the User tweet counts
    #user_counts.pprint()
    for item in user_counts:
        if item in mapLocal:
            mapLocal[item] += 1
        else:
            mapLocal[item] = 1
    mapLocal.pprint()

    # Start Execution of Streams
    ssc.start()
    ssc.awaitTermination()

