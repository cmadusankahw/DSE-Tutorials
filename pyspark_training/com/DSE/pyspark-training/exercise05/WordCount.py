
from pyspark import SparkContext


def printfunc(x):
    print ('Word "%s" occurs %d times' % (x[0], x[1]))


# create spark session
sc = SparkContext("local", "rdd-word-count")

infile = sc.textFile('in/word_count.text')
rdd1 = infile.flatMap(lambda x: x.split())  
rdd2 = rdd1.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
print(rdd2.toDebugString())
rdd2.foreach(printfunc)


