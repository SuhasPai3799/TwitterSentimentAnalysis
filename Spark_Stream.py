#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json
from pyspark.mllib.fpm import FPGrowth
import sys


sc = SparkContext("local[2]", "NetworkWordCount")
sc.setLogLevel("WARN")
home_dir = "/home/suhas"

def f(x):
	f1 = open(home_dir+"/website/NY.txt", "w")
	for ele in x:
		f1.write(ele)
	f1.close()
	FPGrowthAlgo('NY')
def g(x):
	f1 = open(home_dir+"/website/CA.txt", "w")
	for ele in x:
		f1.write(ele)
	f1.close()
	FPGrowthAlgo('CA')
def print_all_tweets(x):
	f = open(home_dir+"/website/live_tweets.txt","w")
	x = reversed(x)
	
	count = 0
	for ele in x:
		cur_string = ele['text']
		l = cur_string.split(" ")
		
		f.write("Tweet#"+str(count)+" ")
		#count  = 0
		count+=1
		for word in l:
			try:
				f.write(word+" ")
			except:
				pass
		f.write("\n")
	f.close()
def print_list(x,city):
	f1 = open(home_dir+"/website/tweets_" + city+".txt", "w")
	print("############################"+str(len(x)))
	for ele in x:
		cur_string = ele['text']
		l = cur_string.split(" ")
		l = set(l)
		for word in set(l):
			if(len(word)>=4):
				try:
					f1.write(word+" ")
				except:
					pass
		f1.write("\n")
	f1.close()

def FPGrowthAlgo(city):
	try:
		data = sc.textFile(home_dir+"/website/tweets_"+city+".txt")
		transactions = data.map(lambda line: line.strip().split(' '))
		model = FPGrowth.train(transactions, minSupport=0.001, numPartitions=1)
		result = model.freqItemsets().collect()
		print("################## FP Growth OP")
		result = sorted(result, key = lambda x : -x[1])
		ones = filter(lambda x : len(x[0])==1,result)
		twos = filter(lambda x : len(x[0])==2,result)
		threes = filter(lambda x : len(x[0])>=3,result)
		f = open(home_dir+"/website/freq_"+city+".txt","w")
		for fi in ones[:3]:
		    f.write(str(fi[0]) + ", " + str(fi[1])+"\n")
		for fi in twos[:3]:
			f.write(str(fi[0]) + ", " + str(fi[1])+"\n")
		for fi in threes[:3]:
			f.write(str(fi[0]) + ", " + str(fi[1])+"\n")
	except Exception as e:
		print('Passed' +  str(e))
		pass			
	


ssc = StreamingContext(sc, 30)
print('Hello')
kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'NY':1,'CA':1})
kafkaStream1 = kafkaStream.window(600,30)

parsed = kafkaStream1.map(lambda v: json.loads(v[1]))

parsed.foreachRDD(lambda x : print_all_tweets(x.collect()))
#parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()

NYstream = parsed.filter(lambda x : x['city']=='NY')
NYstream.count().map(lambda x : 'Tweets from New York Count : %s' %x).pprint()

CAstream = parsed.filter(lambda x : x['city']== 'CA')
CAstream.count().map(lambda x : 'Tweets from California Count : %s' %x).pprint()

NYstream.count().map(lambda x : 'Tweets from New York Count : %s' %x).foreachRDD(lambda x : f(x.collect()))
CAstream.count().map(lambda x : 'Tweets from California Count : %s' %x).foreachRDD(lambda x : g(x.collect()))

NYstream.foreachRDD(lambda x : print_list(x.collect(),'NY'))
CAstream.foreachRDD(lambda x : print_list(x.collect(),'CA'))

ssc.start()
ssc.awaitTermination(timeout=3600)


print('Hello')


