from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("Minimum_Temperature")
sc = SparkContext(conf=conf)

def parseline(line):
    fields = line.split(",")
    station = fields[0]
    temp = fields[3])
    type = fields[2]
    return (station,temp, type)

lines = sc.textFile("/home/vishal/D/Udemy/frank kane/PySpark/2. Spark Basics and the RDD Interface/1800.csv")
rdd = lines.map(parseline)

minTemps = rdd.filter(lambda x : "TMIN" in x[2])
stationTemps = minTemps.map(lambda x :(x[0],int(x[1])))
finalMin = stationTemps.reduceByKey(lambda x,y : min(x,y))
res = finalMin.collect()
for i in res:
    print(i)