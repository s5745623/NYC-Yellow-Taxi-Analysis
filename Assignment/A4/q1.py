#Yuan-Yao Chang yc704
#q1.py
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
top1m = sc.textFile("/Users/yuanyaozhang/Desktop/MassiveData/Datasets/top-1m.csv").cache()
#top1m.count()
aa = open ("q1.txt","w")
aa.write(str(top1m.count()))
aa.close()
