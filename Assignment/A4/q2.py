#Yuan-Yao Chang yc704
#q2.py
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
top1m = sc.textFile("/top-1m.csv")

aa = open ("q2.txt","w")
aa.write(str(top1m.filter(lambda x: x.endswith(".com")).count()))
aa.close()