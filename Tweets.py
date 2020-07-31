import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime, timedelta
import time
import json
def normalize_timestamp(time):
    mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    mytime += timedelta()   # the tweets are timestamped in GMT timezone, while I am in +1 timezone
    return (mytime.strftime("%Y-%m-%d %H:%M:%S"))

producer1 = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda m: json.dumps(m).encode('ascii'))
producer2 = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda m: json.dumps(m).encode('ascii'))
topic_name1 = 'NY'
topic_name2 = 'CA'
NY_location = [-79.7619,40.4774,-71.7956,45.0159]
CA_location = [-124.48,32.53,-114.13,42.01]
access_token = "1255444235421179910-XQwfbYRU0YoY0qqsSQRXneGaN85Gza"
access_token_secret =  "eBeNZrpK8c7iVuCUEtqRks3yieFmQLH5iXMj7rmPLZAUn"
consumer_key =  "Yj0wHPWKToAkOIzbI1Ef4XAu6"
consumer_secret = "YZoyXWzTm8I2ldWghhi4s7jiKx7H3VqXnpmil030lp15IimE6C"


auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth) 


class CustomStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        try:
            
            text = status.text.encode('utf-8')
            loc = status.place.bounding_box.coordinates
            if loc[0][0][0]>=NY_location[0] and loc[0][0][0]<=NY_location[2]:
                
                producer1.send(topic_name1,{'city':"NY",'text':text})
            else:
                producer2.send(topic_name2,{'city':"CA",'text':text})
     
        except Exception as e:
            print(e)
            pass

    def on_error(self, status_code):
        print >> sys.stderr, 'Encountered error with status code:', status_code
        return True # Don't kill the stream

    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        return True # Don't kill the stream

sapi = tweepy.streaming.Stream(auth, CustomStreamListener())    
sapi.filter(locations=[-124.48,32.53,-114.13,42.01,-79.7619,40.4774,-71.7956,45.0159])