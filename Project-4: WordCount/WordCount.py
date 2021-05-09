from pyspark import SparkConf, SparkContext
import re

def normalizeWord(text):
    return re.compile(r'\W+',re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

rdd = sc.textFile("/home/vishal/D/Udemy/frank kane/PySpark/2. Spark Basics and the RDD Interface/10.2 Book.txt")
normalWords = rdd.flatMap(normalizeWord)
words = normalWords.map(lambda x : (x,1)).reduceByKey(lambda x,y : x+y).map(lambda x:(x[1],x[0]))
words = words.sortByKey()
res = words.collect()

for count,word in res:
    cleanword = word.encode('ascii','ignore')
    if cleanword:
        print( word ," : ", count)