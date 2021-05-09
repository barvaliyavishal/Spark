from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Total Spent by Customers")
sc = SparkContext(conf=conf)

def parseLine(line):
    fields = line.split(",")
    Customer = int(fields[0])
    spent = float(fields[2])
    return (Customer,spent)    
    
lines = sc.textFile("/home/vishal/D/Udemy/frank kane/PySpark/2. Spark Basics and the RDD Interface/customer-orders.csv")
rdd = lines.map(parseLine)
vishal = rdd.collect()

totalSpent = rdd.reduceByKey(lambda x,y : (x+y)).map(lambda x : (round(x[1],2),x[0]))

sortedRes = totalSpent.sortByKey()
res = sortedRes.collect()

for i in res:
    print(i) 