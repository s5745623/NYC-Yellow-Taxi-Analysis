#!/usr/bin/env python34
# Simple program to demonstrate spark from the command line
# 
# Run this program with:
# $ spark-submit demo.py

import sys,os,datetime,re,operator
from pyspark.sql import SparkSession
import operator

if __name__=="__main__":
    spark = SparkSession.builder.appName("quazyilx").getOrCreate()
    sc    = spark.sparkContext      # get the context

    # Lower logging
    sc.setLogLevel("ERROR")

    print("You are using Spark {}".format(spark.version))

    # Put the numbers 1 to a million in an RDD and add them
    rdd = sc.parallelize(range(1,1000001))
    print("The numbers 1 to a million added together are: {}".format(
        rdd.reduce(operator.add)))

    # Print the current time with SQL
    now = spark.sql("select now()").take(1)[0][0]
    print("Today is: {}".format(now.isoformat()))


    spark.stop()

