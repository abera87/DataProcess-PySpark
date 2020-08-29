import findspark
findspark.init('/home/ubuntu/spark-3.0.0-bin-hadoop2.7')
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("file:////home/ubuntu/Documents/PySpark/DownloadedData/fakefriends.csv")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

avaragesByAge = totalsByAge.mapValues(lambda x: x[0]/x[1])
results = avaragesByAge.collect()

for result in results:
    print(result)