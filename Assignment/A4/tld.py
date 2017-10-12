#!/usr/bin/env python3.4
#
# Insert your tld.py function below.

#Yuan-Yao Chang yc704
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
top1m = sc.textFile("/top-1m.csv").cache()
def tld(line):

    words = line.split(".")[-1:]
    return words

tlds = top1m.flatMap(lambda line: tld(line))

tlds_and_counts = tlds.countByValue() 
counts_and_tlds = [(count,domain) for (domain,count) in tlds_and_counts.items()] 
counts_and_tlds.sort(reverse=True) 


open("q3_counts.txt","w").write(str(counts_and_tlds[0:50]))