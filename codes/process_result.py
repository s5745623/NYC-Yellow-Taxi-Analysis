import sys,os,datetime,re,operator
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from urllib.request import urlopen
from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.sql.functions import udf,lit
from pyspark.sql.types import *
from datetime import datetime

def process_time_cat(value):
	value =value.strip().split(':')
	hour = float(value[0])
	if hour>=0 and hour <4:
		return "0-4"
	if hour >=4 and hour <8:
		return "4-8"
	if hour >=8 and hour <12:
		return "8-12"

	if hour >=12 and hour <16:
		return "12-16"

	if hour >=16 and hour <20:
		return "16-20"

	if hour >=20 and hour <24:
		return "20-24"
	else :
		return "erro"	

def trans_pred_pickups(value):
	return int(value)

if __name__=="__main__":
	
	spark = SparkSession.builder.appName("data").getOrCreate()
	sc    = spark.sparkContext



	dfDec = spark.read.format('com.databricks.spark.csv').options(header='true').load("/Users/chenyujing/Downloads/result_Friday.csv")

	udf_process_time_cat = udf(process_time_cat, StringType())
	dfDec = dfDec.withColumn("hour_group", udf_process_time_cat("time_cat"))



	tofloat_fri= dfDec.filter(" day_cat = 'Friday' ")

	print (list((set(dfDec.columns))))


	tofloat_fri.cache()



	tofloat_fri.createOrReplaceTempView("dfDec")

	with open("/Users/chenyujing/Downloads/aa/tofloat_fri.txt","w") as f:
		tofloat_fri = spark.sql("select hour_group, latitude, longitude ,  sum(pred_pickups) as a from dfDec group by hour_group, latitude,longitude order by hour_group, a desc").collect()
		for t in tofloat_fri:
			f.write(str(t[0])+','+str(t[1])+','+str(t[2])+','+str(t[3])+'\n')	


	with open("/Users/chenyujing/Downloads/aa/result_0_4.txt","w") as f:
		result_0_4 = spark.sql("select hour_group, latitude, longitude ,  sum(pred_pickups) as a from dfDec where hour_group = '0-4' group by hour_group, latitude,longitude order by hour_group, a desc").collect()
		for t in result_0_4:
			f.write(str(t[0])+','+str(t[1])+','+str(t[2])+','+str(t[3])+'\n')	

	with open("/Users/chenyujing/Downloads/aa/result_4_8.txt","w") as f:
		result_4_8 = spark.sql("select hour_group,  latitude, longitude , sum(pred_pickups) as a from dfDec where hour_group = '4-8'  group by hour_group, latitude,longitude order by hour_group, a desc").collect()
		for t in result_4_8:
			f.write(str(t[0])+','+str(t[1])+','+str(t[2])+','+str(t[3])+'\n')	

	with open("/Users/chenyujing/Downloads/aa/result_8_12.txt","w") as f:
		result_8_12 = spark.sql("select hour_group, latitude, longitude , sum(pred_pickups) as a from dfDec where hour_group = '8-12'   group by hour_group, latitude,longitude order by hour_group, a desc").collect()
		for t in result_8_12:
			f.write(str(t[0])+','+str(t[1])+','+str(t[2])+','+str(t[3])+'\n')	


	with open("/Users/chenyujing/Downloads/aa/result_12_16.txt","w") as f:
		result_12_16 = spark.sql("select hour_group, latitude, longitude ,  sum(pred_pickups) as a from dfDec where hour_group = '12-16'   group by hour_group, latitude,longitude order by hour_group, a desc").collect()
		for t in result_12_16:
			f.write(str(t[0])+','+str(t[1])+','+str(t[2])+','+str(t[3])+'\n')	

	with open("/Users/chenyujing/Downloads/aa/result_16_20.txt","w") as f:
		result_16_20 = spark.sql("select hour_group,  latitude, longitude , sum(pred_pickups) as a from dfDec where hour_group = '16-20'   group by hour_group, latitude,longitude order by hour_group, a desc").collect()
		for t in result_16_20:
			f.write(str(t[0])+','+str(t[1])+','+str(t[2])+','+str(t[3])+'\n')	

	with open("/Users/chenyujing/Downloads/aa/result_20_24.txt","w") as f:
		result_20_24 = spark.sql("select hour_group, latitude, longitude , sum(pred_pickups) as a from dfDec where hour_group = '20-24'   group by hour_group, latitude,longitude order by hour_group, a desc").collect()
		for t in result_20_24:
			f.write(str(t[0])+','+str(t[1])+','+str(t[2])+','+str(t[3])+'\n')	


