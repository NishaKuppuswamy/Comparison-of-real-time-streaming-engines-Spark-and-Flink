import pykafka
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener


# TWITTER API CONFIGURATIONS
access_token = "4342445294-NwYLVWLoiLv9C74Hky7oRFEYHuCkLbe9bVQrGpr"
access_token_secret =  "YZNcnqtf3U8BB6DmdPPOUp7HYN2UrIXHR1RBdwpVcfB9u"
consumer_key =  "gp09Jn6hy4ML8yu4ZLLsreoeq"
consumer_secret =  "eiHSxYS5JXF15n8djuHTqsPoAqxjsoj8BcMMztSfYTdFD27yC3"

# TWITTER API AUTH
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)


# Twitter Stream Listener
class KafkaPushListener(StreamListener):
    def __init__(self):
        # localhost:9092 = Default Zookeeper Producer Host and Port Adresses
        self.client = pykafka.KafkaClient("localhost:9092")

        # Get Producer that has topic name is Twitter
        self.producer = self.client.topics[bytes("tweets-lambda", "ascii")].get_producer()

    def on_data(self, data):
        # Producer produces data for consumer
        # Data comes from Twitter
        self.producer.produce(bytes(data, "ascii"))
        print(data)
        return True

    def on_error(self, status):
        print(status)
        return True


# Twitter Stream Config
twitter_stream = Stream(auth, KafkaPushListener())

# Produce Data that has Game of Thrones hashtag (Tweets)
twitter_stream.filter(track=['#trump'])