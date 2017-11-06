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

def get_if_weekday(value):
	date = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
	date_num= date.weekday()
	if date_num>=0 and date_num<=4:
		return "Weekday"
	elif date_num>=5 and date_num<=6:
		return "Weenkend"



def get_fare_amount(value):
	if value >= 2.5 and value <5:
		return "2.5-5"
	elif value >= 5 and value < 10:
		return "5-10"
	elif value >=10 and value <15:
		return "10-15"
	elif value >=15 and value <20:
		return "15-20"
	elif value >=20 and value <25:
		return "20-25"
	elif value >=25 and value <30:
		return "25-30"
	elif value >=30 and value <35:
		return "30-35"
	elif value >=35 and value <40:
		return "35-40"
	elif value >=40 and value <45:
		return "40-45"
	else:
		return "Invalid"

def get_tip_amount(value):
	if value > 0 and value <=1:
		return "0-1"
	elif value > 1 and value <=2:
		return "1-2"
	elif value > 2 and value <=3:
		return "2-3"
	elif value >3 and value <=4:
		return "3-4"
	elif value >4 and value <=5:
		return "4-5"
	elif value >5 and value <=6:
		return "5-6"
	elif value >6 and value <=7:
		return "6-7"
	elif value >7 and value <=8:
		return "7-8"
	elif value >8 and value <=9:
		return "8-9"
	elif value >9 and value <=10:
		return "9-10"
	elif value >10 and value <=15:
		return "10-15"
	elif value >15 and value <=20:
		return "15-20"
	else:
		return "Invalid"


def get_month(value):
	month = int(value[5:7])
	return month



if __name__=="__main__":
	
	spark = SparkSession.builder.appName("data").getOrCreate()
	sc    = spark.sparkContext



	dfDec = spark.read.format('com.databricks.spark.csv').options(header='true').load("/Users/chenyujing/Downloads/dataset/1-6/*.csv")
	# dfDec2 = spark.read.format('com.databricks.spark.csv').options(header='true').load("/Users/chenyujing/Downloads/dataset/6-12/*.csv")
	# dfDec3 = spark.read.format('com.databricks.spark.csv').options(header='true').load("/taxi+_zone_lookup.csv")
	# dfDec = df1.unionAll(df2).unionAll(df3).unionAll(df4).unionAll(df5).unionAll(df6)

	# dfDec2 = df7.unionAll(df8).unionAll(df9).unionAll(df10).unionAll(df11).unionAll(df12)


	dfDec = dfDec.withColumn("fare_amount",dfDec["fare_amount"].cast("double")).withColumn("tip_amount",dfDec["tip_amount"].cast("double")).withColumn("dropoff_latitude",dfDec["dropoff_latitude"].cast("double")).withColumn("dropoff_longitude",dfDec["dropoff_longitude"].cast("double")).withColumn("pickup_latitude",dfDec["pickup_latitude"].cast("double")).withColumn("pickup_longitude",dfDec["pickup_longitude"].cast("double"))


	tofloat = dfDec.filter("dropoff_latitude >= 40.5 and  dropoff_latitude <=41").filter("pickup_latitude >=40.5 and pickup_latitude <=41").filter("pickup_longitude >= -74.5 and pickup_longitude <= -71.5").filter("dropoff_longitude >= -74.5 and dropoff_longitude <= -71.5")

	# dfDec2 = spark.read.format('com.databricks.spark.csv').options(header='true').load("Untitled.csv")
	



	# dfDec.write.csv("/Users/chenyujing/Downloads/dataset/result.csv")
	# dfDec.show()

	# tofloat = dfDec.withColumn("fare_amount",dfDec["fare_amount"].cast("double")).withColumn("tip_amount",dfDec["tip_amount"].cast("double")).withColumn("dropoff_latitude",dfDec["dropoff_latitude"].cast("double")).withColumn("dropoff_longitude",dfDec["dropoff_longitude"].cast("double")).withColumn("pickup_latitude",dfDec["pickup_latitude"].cast("double")).withColumn("pickup_longitude",dfDec["pickup_longitude"].cast("double"))

	# tofloat = tofloat.filter("dropoff_latitude != 0 ").filter("dropoff_longitude != 0 " ).filter("pickup_latitude != 0 " ).filter("pickup_longitude != 0 ")


	# tofloat = tofloat.filter("dropoff_latitude >= 40.5 and  dropoff_latitude <=41").filter("pickup_latitude >=40.5 and pickup_latitude <=41").filter("pickup_longitude >= -74.5 and pickup_longitude <= -71.5").filter("dropoff_longitude >= -74.5 and dropoff_longitude <= -71.5")

	udf_get_pickup_hour = udf(get_pickup_hour, StringType())
	tofloat = tofloat.withColumn("pickup_hours", udf_get_pickup_hour("tpep_pickup_datetime"))
	# tofloat = tofloat.withColumn("dropoff_hours", udf_get_pickup_hour("tpep_dropoff_datetime"))

	udf_get_weekday = udf(get_weekday, StringType())
	tofloat = tofloat.withColumn("pickup_weekday", udf_get_weekday("tpep_pickup_datetime"))


	udf_get_if_weekday = udf(get_if_weekday, StringType())
	tofloat = tofloat.withColumn("if_weekday", udf_get_if_weekday("tpep_pickup_datetime"))


	# udf_get_fare_amount = udf(get_fare_amount, StringType())
	# tofloat = tofloat.withColumn("fare_amount_group", udf_get_fare_amount("fare_amount"))

	
	# udf_get_tip_amount = udf(get_tip_amount, StringType())
	# tofloat = tofloat.withColumn("tip_amount_group", udf_get_tip_amount("tip_amount"))


	udf_get_month = udf(get_month, StringType())
	tofloat = tofloat.withColumn("month", udf_get_month("tpep_pickup_datetime"))


	# tofloat = tofloat.filter(" pickup_weekday = 'Mon' ")

	# tofloat_mon= tofloat.filter(" pickup_weekday = 'Mon' ")
	# tofloat_tue= tofloat.filter(" pickup_weekday = 'Tue' ")
	# tofloat_wen= tofloat.filter(" pickup_weekday = 'Wen' ")
	# tofloat_Tru= tofloat.filter(" pickup_weekday = 'Tru' ")
	tofloat_fri= tofloat.filter(" pickup_weekday = 'Fri' ")
	# tofloat_sat= tofloat.filter(" pickup_weekday = 'Sat' ")
	# tofloat_sun= tofloat.filter(" pickup_weekday = 'Sun' ")

	tofloat_fri.cache()

	result_0_4 = tofloat_fri.filter("pickup_hours = '0-2' or pickup_hours = '2-4' ")
	result_4_8 = tofloat_fri.filter("pickup_hours = '4-6' or pickup_hours = '6-8' ")
	result_8_12 = tofloat_fri.filter("pickup_hours = '8-10' or pickup_hours = '10-12' ")
	result_12_16 = tofloat_fri.filter("pickup_hours = '12-14' or pickup_hours = '14-16' ")
	result_16_20 = tofloat_fri.filter("pickup_hours = '16-18' or pickup_hours = '18-20' ")
	result_20_24 = tofloat_fri.filter("pickup_hours = '20-22' or pickup_hours = '22-24' ")


	result_0_4.write.format("com.databricks.spark.csv").options(header='true').save("/Users/chenyujing/Downloads/Fri_0_4")
	result_4_8.write.format("com.databricks.spark.csv").options(header='true').save("/Users/chenyujing/Downloads/Fri_4_8")
	result_8_12.write.format("com.databricks.spark.csv").options(header='true').save("/Users/chenyujing/Downloads/Fri_8_12")
	result_12_16.write.format("com.databricks.spark.csv").options(header='true').save("/Users/chenyujing/Downloads/Fri_12_16")
	result_16_20.write.format("com.databricks.spark.csv").options(header='true').save("/Users/chenyujing/Downloads/Fri_16_20")
	result_20_24.write.format("com.databricks.spark.csv").options(header='true').save("/Users/chenyujing/Downloads/Fri_20_24")











	# tofloat_mon.write.format("com.databricks.spark.csv").options(header='true').save("/Users/chenyujing/Downloads/2016-mon")
	# tofloat_tue.write.format("com.databricks.spark.csv").options(header='true').save("/Users/chenyujing/Downloads/2016-tue")
	# tofloat_wen.write.format("com.databricks.spark.csv").options(header='true').save("/Users/chenyujing/Downloads/2016-wen")
	# tofloat_Tru.write.format("com.databricks.spark.csv").options(header='true').save("/Users/chenyujing/Downloads/2016-tru")
	# tofloat_fri.write.format("com.databricks.spark.csv").options(header='true').save("/Users/chenyujing/Downloads/2016-fri")
	# tofloat_sat.write.format("com.databricks.spark.csv").options(header='true').save("/Users/chenyujing/Downloads/2016-sat")
	# tofloat_sun.write.format("com.databricks.spark.csv").options(header='true').save("/Users/chenyujing/Downloads/2016-sun")
