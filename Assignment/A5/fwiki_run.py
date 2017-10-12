#!/usr/bin/env python3
#
# Run this program with spark-submit

import sys,os,datetime,re,operator
from pyspark import SparkContext, SparkConf
from fwiki import LogLine

QUERIES = [["output","SELECT SUBSTR(datetime,1,7) as month,COUNT(1) from logs GROUP BY 1 order by month"]]

if __name__=="__main__":
    # Get your sparkcontext and make a dataframe
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("quazyilx").getOrCreate()
    sc    = spark.sparkContext      # get the context
    
    # Create an RDD from s3://gu-anly502/logs/forensicswiki.2012.txt
    url = "s3://gu-anly502/logs/forensicswiki.2012.txt"
    # NOTE: Do this with 1 master m3.xlarge, 2 core m3.xlarge, and 4 task m3.xlarge
    # otherwise it will take forever...
    
    loglines = sc.textFile("/Users/yuanyaozhang/Desktop/MassiveData/HW/A5/forensicswiki.2012.txt").cache()
    #loglines = sc.textFile("/Users/yuanyaozhang/Desktop/MassiveData/HW/A5/123.txt").cache()
    logs     = loglines.map(lambda l:LogLine(l).row()).cache()
    df       = spark.createDataFrame(logs).cache()
    df.createOrReplaceTempView("logs")
    
    # Register the dataframe as an SQL table called 'logs'

    # Print how many log lines there are
    print("Total Log Lines: {}".format(spark.sql("select count(*) from logs").collect()))

    # Figure out when it started and ended
    (start,end) = spark.sql("select min(datetime),max(datetime) from logs").collect()[0]

    print("Date range: {} to {}".format(start,end))

    # Now generate the requested output
    with open("fwiki_run.txt", "w") as f: 
        for (start,end) in QUERIES:
            query_result = spark.sql(end).collect()
            #print("{}: {}".format(start,query_result))
            for month,count in query_result:
                if month != None: #handling the None
                    f.write(str(month) + "\t" +str(count)+ "\n" )    
                
