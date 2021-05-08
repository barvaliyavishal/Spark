from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("AverageFriends")
sc = SparkContext(conf=conf)


def parseline(line):
    fields = line.split(",")
    age = int(fields[2])
    friends = int(fields[3])
    return (age,friends)

lines = sc.textFile("/home/vishal/D/Udemy/frank kane/PySpark/2. Spark Basics and the RDD Interface/fakefriends.csv")
AgeFriendsRDD = lines.mapValues(lambda x: (x,1))
TotalsByAge = AgeFriendsRDD.reduceByKey(lambda x,y:(x[0]+y[0], x[1]+y[1]))
AvgByAge = TotalsByAge.mapValues(lambda x,y : (x / y))
result = AvgByAge.collect()

for i in result:
    print(i)
