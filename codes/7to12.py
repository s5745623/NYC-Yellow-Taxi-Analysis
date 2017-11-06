
import sys,os,datetime,re,operator
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from urllib.request import urlopen
from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.sql.functions import udf,lit
from pyspark.sql.types import *
from datetime import datetime




def get_pickup_hour(value):
	hour = float(value[11:13])
	if hour>=0 and hour <2:
		return "0-2"
	if hour >=2 and hour <4:
		return "2-4"
	if hour >=4 and hour <6:
		return "4-6"
	if hour >=6 and hour <8:
		return "6-8"
	if hour >=8 and hour <10:
		return "8-10"
	if hour >=10 and hour <12:
		return "10-12"
	if hour >=12 and hour <14:
		return "12-14"
	if hour >=14 and hour <16:
		return "14-16"
	if hour >=16 and hour <18:
		return "16-18"
	if hour >=18 and hour <20:
		return "18-20"
	if hour >=20 and hour <22:
		return "20-22"
	if hour >=22 and hour <24:
		return "22-24"
	else :
		return "erro"

def get_weekday(value):
	date = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
	date_num= date.weekday()
	if date_num==0:
		return "Mon"
	if date_num==1:
		return "Tue"
	if date_num==2:
		return "Wed"
	if date_num==3:
		return "Thu"
	if date_num==4:
		return "Fri"
	if date_num==5:
		return "Sat"
	if date_num ==6:
		return "Sun"




if __name__=="__main__":
	
	spark = SparkSession.builder.appName("data").getOrCreate()
	sc    = spark.sparkContext



	dfDec = spark.read.format('com.databricks.spark.csv').options(header='true').load("/Users/chenyujing/Downloads/dataset/6-12/*.csv")
	# dfDec2 = spark.read.format('com.databricks.spark.csv').options(header='true').load("/Users/chenyujing/Downloads/dataset/6-12/*.csv")
	dfDec3 = spark.read.format('com.databricks.spark.csv').options(header='true').load("/Users/chenyujing/Downloads/taxi+_zone_lookup.csv")
	# dfDec = df1.unionAll(df2).unionAll(df3).unionAll(df4).unionAll(df5).unionAll(df6)

	# dfDec2 = df7.unionAll(df8).unionAll(df9).unionAll(df10).unionAll(df11).unionAll(df12)


	dfDec = dfDec.withColumn("fare_amount",dfDec["fare_amount"].cast("double")).withColumn("tip_amount",dfDec["tip_amount"].cast("double"))



	dfDec = dfDec.withColumn("PULocationID",dfDec["PULocationID"].cast("double")).withColumn("DOLocationID",dfDec["DOLocationID"].cast("double"))
	dfDec = dfDec.filter("PULocationID != 264 and  PULocationID != 265").filter("DOLocationID != 264 and  DOLocationID != 265")

	# dfDec2 = spark.read.format('com.databricks.spark.csv').options(header='true').load("Untitled.csv")
	


	# dfDec.write.csv("/Users/chenyujing/Downloads/dataset/result.csv")
	# dfDec.show()

	# tofloat = dfDec.withColumn("fare_amount",dfDec["fare_amount"].cast("double")).withColumn("tip_amount",dfDec["tip_amount"].cast("double")).withColumn("dropoff_latitude",dfDec["dropoff_latitude"].cast("double")).withColumn("dropoff_longitude",dfDec["dropoff_longitude"].cast("double")).withColumn("pickup_latitude",dfDec["pickup_latitude"].cast("double")).withColumn("pickup_longitude",dfDec["pickup_longitude"].cast("double"))

	# tofloat = tofloat.filter("dropoff_latitude != 0 ").filter("dropoff_longitude != 0 " ).filter("pickup_latitude != 0 " ).filter("pickup_longitude != 0 ")


	# tofloat = tofloat.filter("dropoff_latitude >= 40.5 and  dropoff_latitude <=41").filter("pickup_latitude >=40.5 and pickup_latitude <=41").filter("pickup_longitude >= -74.5 and pickup_longitude <= -71.5").filter("dropoff_longitude >= -74.5 and dropoff_longitude <= -71.5")

	# udf_get_pickup_hour = udf(get_pickup_hour, StringType())
	# dfDec = dfDec.withColumn("pickup_hours", udf_get_pickup_hour("tpep_pickup_datetime"))

	# udf_get_weekday = udf(get_weekday, StringType())
	# dfDec = dfDec.withColumn("pickup_weekday", udf_get_weekday("tpep_pickup_datetime"))


	# udf_get_if_weekday = udf(get_if_weekday, StringType())
	# tofloat = tofloat.withColumn("if_weekday", udf_get_if_weekday("tpep_pickup_datetime"))


	# udf_get_fare_amount = udf(get_fare_amount, StringType())
	# tofloat = tofloat.withColumn("fare_amount_group", udf_get_fare_amount("fare_amount"))

	
	# udf_get_tip_amount = udf(get_tip_amount, StringType())
	# tofloat = tofloat.withColumn("tip_amount_group", udf_get_tip_amount("tip_amount"))


	# udf_get_month = udf(get_month, StringType())
	# tofloat = tofloat.withColumn("month", udf_get_month("tpep_pickup_datetime"))


	
	# tofloat_fri= dfDec.filter(" pickup_weekday = 'Fri' ")

	tofloat_fri = dfDec.join(dfDec3, dfDec.PULocationID == dfDec3.LocationID, 'left')

	tofloat_fri.cache()

	tofloat_fri.createOrReplaceTempView("dfDec")

	# tofloat_sat= tofloat.filter(" pickup_weekday = 'Sat' ")
	# tofloat_sun= tofloat.filter(" pickup_weekday = 'Sun' ")

	# with open("result/area_0_4.txt","w") as f:
	# 	area_0_4 = spark.sql("select Zone, count(1) as b from dfDec where pickup_hours = '0-2' or pickup_hours = '2-4'  group by Zone  order by b desc").collect()
	# 	for t in area_0_4:
	# 		f.write(str(t[0])+'\t'+str(t[1])+'\n')

	# with open("result/area_4_8.txt","w") as f:
	# 	area_4_8 = spark.sql("select Zone, count(1) as b from dfDec where pickup_hours = '4-6' or pickup_hours = '6-8'  group by Zone  order by b desc").collect()
	# 	for t in area_4_8:
	# 		f.write(str(t[0])+'\t'+str(t[1])+'\n')
	# with open("result/area_8_12.txt","w") as f:
	# 	area_8_12 = spark.sql("select Zone, count(1) as b from dfDec where pickup_hours = '8-10' or pickup_hours = '10-12'  group by Zone  order by b desc").collect()
	# 	for t in area_8_12:
	# 		f.write(str(t[0])+'\t'+str(t[1])+'\n')
	# with open("result/area_12_16.txt","w") as f:
	# 	area_12_16 = spark.sql("select Zone, count(1) as b from dfDec where pickup_hours = '12-14' or pickup_hours = '14-16'  group by Zone  order by b desc").collect()
	# 	for t in area_12_16:
	# 		f.write(str(t[0])+'\t'+str(t[1])+'\n')
	# with open("result/area_16_20.txt","w") as f:
	# 	area_16_20 = spark.sql("select Zone, count(1) as b from dfDec where pickup_hours = '16-18' or pickup_hours = '18-20'  group by Zone  order by b desc").collect()
	# 	for t in area_16_20:
	# 		f.write(str(t[0])+'\t'+str(t[1])+'\n')
	# with open("result/area_20_24.txt","w") as f:
	# 	area_20_24 = spark.sql("select Zone, count(1) as b from dfDec where pickup_hours = '20-22' or pickup_hours = '22-24'  group by Zone  order by b desc").collect()
	# 	for t in area_20_24:
	# 		f.write(str(t[0])+'\t'+str(t[1])+'\n')


	with open("result/area_all.txt","w") as f:
		area_fri = spark.sql("select Zone, count(1) as b from dfDec group by Zone  order by b desc").collect()
		for t in area_fri:
			f.write(str(t[0])+'\t'+str(t[1])+'\n')


