#!/usr/bin/env python3
#
# Run this program with spark-submit

import sys,os,datetime,re,operator
from pyspark import SparkContext, SparkConf
from quazyilx import Quazyilx

INSERT_YOUR_CODE_HERE = ""

QUERIES = [["total_rows","select count(*) from quazyilx"],
           ["total_errors","select count(*) from quazyilx where fnard = -1 and fnok = -1 and cark = -1 and gnuck = -1"], # fnard==fnok==cark==-1
           ["one_error_others_gt5","select count(*) from quazyilx where fnard = -1 and fnok > 5 and cark > 5"], # fnard==-1, fnok>5, 
           ["first_date","select min(datetime) from quazyilx"],   
           ["last_date","select max(datetime) from quazyilx"],
           ["first_error_date","select min(datetime) from quazyilx b where fnard = -1 and fnok = -1 and cark = -1 and gnuck = -1"],
           ["last_error_date","select max(datetime) from quazyilx b where fnard = -1 and fnok = -1 and cark = -1 and gnuck = -1"]
]



if __name__=="__main__":
    # Get your sparkcontext and make a dataframe
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("quazyilx").getOrCreate()
    sc = spark.sparkContext      # get the context
    
    #lines = sc.textFile("/Users/yuanyaozhang/Desktop/MassiveData/HW/A5/quazyilx0.txt").cache()
    lines = sc.textFile("s3://gu-anly502/A1/quazyilx1.txt").cache()
    line = lines.map(lambda l:Quazyilx(l).Row()).cache()
    df = spark.createDataFrame(line).cache()
    df.createOrReplaceTempView("quazyilx")
    
    
    # Replace this code with your own
    #print("*** Verifying that Spark works ***",file=sys.stderr)
    #print("*** Result = {}  (should be 499500) ***".format(res),file=sys.stderr)
    #assert res==499500

    # Create an RDD from s3://gu-anly502/A1/quazyilx1.txt
    # NOTE: Do this with 1 master m3.xlarge, 2 core m3.xlarge, and 4 task m3.xlarge
    # otherwise it will take forever...
    
    # register your dataframe as the SQL table quazyilx
    # You probably want to cache it, also!

    # Print how many rows we have
    #print("rows: {}".format(spark.sql("select count(*) from quazyilx").collect()))

    # Now do the queries
    with open("quazyilx_run.txt", "w") as f: 
        for (var,query) in QUERIES:
            print("{}-query: {}".format(var,query))
            f.write(var+'-query: '+query+"\n")
            if query:
                query_result = spark.sql(query).collect()
                print("{}: {}".format(var,query_result))
                f.write(var+':   ')
                for i in query_result:
                    f.write(str(i)+"\n\n")
