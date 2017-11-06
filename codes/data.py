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

	df1 = spark.read.format('com.databricks.spark.csv').options(header='true').load("s3://nyc-tlc/trip\ data/yellow_tripdata_2016-01.csv")
	df2 = spark.read.format('com.databricks.spark.csv').options(header='true').load("s3://nyc-tlc/trip\ data/yellow_tripdata_2016-02.csv")
	df3 = spark.read.format('com.databricks.spark.csv').options(header='true').load("s3://nyc-tlc/trip\ data/yellow_tripdata_2016-03.csv")
	df4 = spark.read.format('com.databricks.spark.csv').options(header='true').load("s3://nyc-tlc/trip\ data/yellow_tripdata_2016-04.csv")
	df5 = spark.read.format('com.databricks.spark.csv').options(header='true').load("s3://nyc-tlc/trip\ data/yellow_tripdata_2016-05.csv")
	df6 = spark.read.format('com.databricks.spark.csv').options(header='true').load("s3://nyc-tlc/trip\ data/yellow_tripdata_2016-06.csv")
	df7 = spark.read.format('com.databricks.spark.csv').options(header='true').load("s3://nyc-tlc/trip\ data/yellow_tripdata_2016-07.csv")
	df8 = spark.read.format('com.databricks.spark.csv').options(header='true').load("s3://nyc-tlc/trip\ data/yellow_tripdata_2016-08.csv")
	df9 = spark.read.format('com.databricks.spark.csv').options(header='true').load("s3://nyc-tlc/trip\ data/yellow_tripdata_2016-09.csv")
	df10 = spark.read.format('com.databricks.spark.csv').options(header='true').load("s3://nyc-tlc/trip\ data/yellow_tripdata_2016-10.csv")
	df11 = spark.read.format('com.databricks.spark.csv').options(header='true').load("s3://nyc-tlc/trip\ data/yellow_tripdata_2016-11.csv")
	df12 = spark.read.format('com.databricks.spark.csv').options(header='true').load("s3://nyc-tlc/trip\ data/yellow_tripdata_2016-12.csv")


	# dfDec = spark.read.format('com.databricks.spark.csv').options(header='true').load("/Users/chenyujing/Downloads/dataset/1-6/*.csv")
	# dfDec2 = spark.read.format('com.databricks.spark.csv').options(header='true').load("/Users/chenyujing/Downloads/dataset/6-12/*.csv")
	dfDec3 = spark.read.format('com.databricks.spark.csv').options(header='true').load("/taxi+_zone_lookup.csv")
	dfDec = df1.unionAll(df2).unionAll(df3).unionAll(df4).unionAll(df5).unionAll(df6)

	dfDec2 = df7.unionAll(df8).unionAll(df9).unionAll(df10).unionAll(df11).unionAll(df12)


	cols= list(set(dfDec.columns + dfDec2.columns))
	print (cols)
	dfDec = dfDec.withColumn("PULocationID",  lit("9999")).withColumn("DOLocationID",lit("9999")).select(cols)
	dfDec2 = dfDec2.withColumn("pickup_longitude"  ,lit("9999")).withColumn("pickup_latitude"  ,lit("9999")).withColumn("dropoff_longitude"  ,lit("9999")).withColumn("dropoff_latitude"  ,lit("9999")).select(cols)
	dfDec = dfDec.withColumn("fare_amount",dfDec["fare_amount"].cast("double")).withColumn("tip_amount",dfDec["tip_amount"].cast("double")).withColumn("dropoff_latitude",dfDec["dropoff_latitude"].cast("double")).withColumn("dropoff_longitude",dfDec["dropoff_longitude"].cast("double")).withColumn("pickup_latitude",dfDec["pickup_latitude"].cast("double")).withColumn("pickup_longitude",dfDec["pickup_longitude"].cast("double"))


	dfDec2 = dfDec2.withColumn("PULocationID",dfDec2["PULocationID"].cast("double")).withColumn("DOLocationID",dfDec2["DOLocationID"].cast("double"))
	dfDec2 = dfDec2.filter("PULocationID != 264 and  PULocationID != 265").filter("DOLocationID != 264 and  DOLocationID != 265")

	dfDec = dfDec.filter("dropoff_latitude >= 40.5 and  dropoff_latitude <=41").filter("pickup_latitude >=40.5 and pickup_latitude <=41").filter("pickup_longitude >= -74.5 and pickup_longitude <= -71.5").filter("dropoff_longitude >= -74.5 and dropoff_longitude <= -71.5")

	# dfDec2 = spark.read.format('com.databricks.spark.csv').options(header='true').load("Untitled.csv")
	
	dfDec = dfDec.unionAll(dfDec2)

	tofloat = dfDec.join(dfDec3, dfDec.PULocationID == dfDec3.LocationID, 'left')


	udf_get_pickup_hour = udf(get_pickup_hour, StringType())
	tofloat = tofloat.withColumn("pickup_hours", udf_get_pickup_hour("tpep_pickup_datetime"))
	tofloat = tofloat.withColumn("dropoff_hours", udf_get_pickup_hour("tpep_dropoff_datetime"))

	udf_get_weekday = udf(get_weekday, StringType())
	tofloat = tofloat.withColumn("pickup_weekday", udf_get_weekday("tpep_pickup_datetime"))


	# udf_get_if_weekday = udf(get_if_weekday, StringType())
	# tofloat = tofloat.withColumn("if_weekday", udf_get_if_weekday("tpep_pickup_datetime"))


	# udf_get_fare_amount = udf(get_fare_amount, StringType())
	# tofloat = tofloat.withColumn("fare_amount_group", udf_get_fare_amount("fare_amount"))

	
	# udf_get_tip_amount = udf(get_tip_amount, StringType())
	# tofloat = tofloat.withColumn("tip_amount_group", udf_get_tip_amount("tip_amount"))


	udf_get_month = udf(get_month, StringType())
	tofloat = tofloat.withColumn("month", udf_get_month("tpep_pickup_datetime"))


	# tofloat.write.format("com.databricks.spark.csv").options(header='true').save("/Users/chenyujing/Downloads/dataset/result")

	tofloat.cache()
	
	tofloat.createOrReplaceTempView("dfDec")

	
	weekdays = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
	with open("/home/hadoop/anly502_2017_spring/Final project/result/hours_count.txt","w") as f:
		for i in weekdays:
			hours_count = spark.sql("select pickup_hours,count(1) from dfDec where pickup_weekday=\"{aa}\" group by pickup_hours".format(aa=i)).collect()
			for t in hours_count:
				f.write(i+"\t"+str(t[0])+'\t'+str(t[1])+'\n')	
	with open("/home/hadoop/anly502_2017_spring/Final project/result/hours_sum.txt","w") as f:
		for i in weekdays:
			hours_sum = spark.sql("select pickup_hours,sum(fare_amount) from dfDec where pickup_weekday=\"{aa}\" group by pickup_hours".format(aa=i)).collect()
			for t in hours_sum:
				f.write(i+"\t"+str(t[0])+'\t'+str(t[1])+'\n')

	months = ["1","2","3",'4','5','6','7','8','9','10','11','12']
	with open("/home/hadoop/anly502_2017_spring/Final project/result/months_count.txt","w") as f:
		for i in months:
			months_count = spark.sql("select pickup_weekday,count(1) from dfDec where month=\"{aa}\" group by pickup_weekday".format(aa=i)).collect()
			for t in months_count:
				f.write(i+"\t"+str(t[0])+'\t'+str(t[1])+'\n')
	with open("/home/hadoop/anly502_2017_spring/Final project/result/months_sum.txt","w") as f:
		for i in months:
			months_sum = spark.sql("select pickup_weekday,sum(fare_amount) from dfDec where month=\"{aa}\" group by pickup_weekday".format(aa=i)).collect()
			for t in months_sum:
				f.write(i+"\t"+str(t[0])+'\t'+str(t[1])+'\n')

	with open("/home/hadoop/anly502_2017_spring/Final project/result/hours_distance_totalamount.txt","w") as f:
		hours_distance_totalamount = spark.sql("select pickup_hours,sum(trip_distance),sum(total_amount) from dfDec group by pickup_hours").collect()
		for i in hours_distance_totalamount:
			f.write(str(i[0])+"\t"+str(i[1])+"\t"+str(i[2])+"\n")

#  tips
	with open("/home/hadoop/anly502_2017_spring/Final project/result/tips_hours_count.txt","w") as f:
		for i in weekdays:
			tips_hours_count = spark.sql("select pickup_hours,count(1) from dfDec where pickup_weekday=\"{aa}\"  and tip_amount != 0 group by pickup_hours".format(aa=i)).collect()
			for t in tips_hours_count:
				f.write(i+"\t"+str(t[0])+'\t'+str(t[1])+'\n')	
	with open("/home/hadoop/anly502_2017_spring/Final project/result/tips_hours_sum.txt","w") as f:
		for i in weekdays:
			tips_hours_sum = spark.sql("select pickup_hours,sum(tip_amount) from dfDec where pickup_weekday=\"{aa}\" group by pickup_hours".format(aa=i)).collect()
			for t in tips_hours_sum:
				f.write(i+"\t"+str(t[0])+'\t'+str(t[1])+'\n')

	with open("/home/hadoop/anly502_2017_spring/Final project/result/tips_pickup_area_sum.txt","w") as f:
		tips_pickup_area_sum = spark.sql("select Zone,sum(tip_amount) from dfDec group by Zone order by Zone").collect()
		for t in tips_pickup_area_sum:
			f.write(str(t[0])+'\t'+str(t[1])+'\n')


	with open("/home/hadoop/anly502_2017_spring/Final project/result/fare_weekdays_amount.txt","w") as f:
		fare_weekdays_amount = spark.sql("select pickup_weekday, count(1) from dfDec group by pickup_weekday").collect()
		for t in fare_weekdays_amount:
			f.write(str(t[0])+'\t'+str(t[1])+'\n')

	with open("/home/hadoop/anly502_2017_spring/Final project/result/fare_weekdays_sum.txt","w") as f:
		fare_weekdays_sum = spark.sql("select pickup_weekday, sum(fare_amount) from dfDec group by pickup_weekday").collect()
		for t in fare_weekdays_sum:
			f.write(str(t[0])+'\t'+str(t[1])+'\n')

	with open("/home/hadoop/anly502_2017_spring/Final project/result/tips_weekdays_sum.txt","w") as f:
		tips_weekdays_sum = spark.sql("select pickup_weekday, sum(tip_amount) from dfDec group by pickup_weekday").collect()
		for t in tips_weekdays_sum:
			f.write(str(t[0])+'\t'+str(t[1])+'\n')
	with open("/home/hadoop/anly502_2017_spring/Final project/result/tips_weekdays_amount.txt","w") as f:
		tips_weekdays_amount = spark.sql("select pickup_weekday, count(1) from dfDec where tip_amount != 0 group by pickup_weekday ").collect()
		for t in tips_weekdays_amount:
			f.write(str(t[0])+'\t'+str(t[1])+'\n')	


	with open("/home/hadoop/anly502_2017_spring/Final project/result/tips_months_count.txt","w") as f:
		for i in months:
			tips_months_count = spark.sql("select pickup_weekday,count(1) from dfDec where month=\"{aa}\" and tip_amount != 0 group by pickup_weekday".format(aa=i)).collect()
			for t in tips_months_count:
				f.write(i+"\t"+str(t[0])+'\t'+str(t[1])+'\n')

	with open("/home/hadoop/anly502_2017_spring/Final project/result/tips_months_sum.txt","w") as f:
		for i in months:
			tips_months_sum = spark.sql("select pickup_weekday,sum(tip_amount) from dfDec where month=\"{aa}\" group by pickup_weekday".format(aa=i)).collect()
			for t in tips_months_sum:
				f.write(i+"\t"+str(t[0])+'\t'+str(t[1])+'\n')




	with open("/home/hadoop/anly502_2017_spring/Final project/result/fares_pickup_area_sum.txt") as f:
		fares_pickup_area_sum = spark.sql("select Zone,sum(fare_amount) from dfDec group by Zone order by Zone").collect()
		for t in fares_pickup_area_sum:
			f.write(str(t[0])+'\t'+str(t[1])+'\n')
	
	with open("/home/hadoop/anly502_2017_spring/Final project/result/tips_dropoff_area_sum.txt") as f:
		tips_dropoff_area_sum = spark.sql("select DOLocationID,sum(tip_amount) from dfDec group by DOLocationID order by DOLocationID")
		for t in tips_dropoff_area_sum:
			f.write(str(t[0])+'\t'+str(t[1])+'\n')












	# weekdays = ["Mon","Tue","Wen","Tru","Fri","Sat","Sun"]
	# for i in weekdays:
	# 	result = spark.sql("select count(*) from dfDec where pickup_weekday=\"{aa}\"".format(aa=i)).collect()
	# 	print (i+"\t"+str(result[0][0])+'\n')





	############## query result ##############

	# weekday_avg = spark.sql("select pickup_weekday,avg(fare_amount),avg(tip_amount) from dfDec group by pickup_weekday").collect()
	# pickup_hours_avg = spark.sql("select pickup_hours,avg(fare_amount),avg(tip_amount) from dfDec group by pickup_hours").collect()
	# fare_group = spark.sql("select fare_amount_group,count(1) from dfDec group by fare_amount_group").collect()
	# tips_group = spark.sql("select tip_amount_group,count(1) from dfDec group by tip_amount_group").collect()
	# tota_num = spark.sql("select count(*) from dfDec").collect()
	# tips_num = spark.sql("select count(*) from dfDec where tip_amount !=0").collect()
	
	# month_num = spark.sql("select month,count(1) from dfDec group by month order by month").collect()
	# with open("result/month_num.txt","w") as f:
	#     for i in month_num:
	#         f.write(str(i[0])+"\t"+str(i[1])+"\n")

	# week_num = spark.sql("select pickup_weekday,count(1) from dfDec group by pickup_weekday").collect()
	# with open("result/week_data.txt","w") as f:
	# 	for i in week_num:
	# 		f.write(str(i[0])+"\t"+str(i[1])+"\n")

	# month_ave_fare = spark.sql("select month,avg(fare_amount) from dfDec group by month order by month").collect()
	# with open("result/month_ave_fare.txt","w") as f:
	# 	for i in month_ave_fare:
	# 		f.write(str(i[0])+"\t"+str(i[1])+"\n")

	# week_ave_fare = spark.sql("select pickup_weekday,avg(fare_amount) from dfDec group by pickup_weekday order by pickup_weekday").collect()
	# with open("result/week_ave_fare.txt","w") as f:
	# 	for i in week_ave_fare:
	# 		f.write(str(i[0])+"\t"+str(i[1])+"\n")

	# month_sum_fare = spark.sql("select month,sum(fare_amount) from dfDec group by month order by month").collect()
	# with open("result/month_sum_fare.txt","w") as f:
	# 	for i in month_sum_fare:
	# 		f.write(str(i[0])+"\t"+str(i[1])+"\n")

	# week_sum_fare = spark.sql("select pickup_weekday,sum(fare_amount) from dfDec group by pickup_weekday order by pickup_weekday").collect()
	# with open("result/week_sum_fare.txt","w") as f:
	# 	for i in week_sum_fare:
	# 		f.write(str(i[0])+"\t"+str(i[1])+"\n")

	# fare_group = spark.sql("select fare_amount_group,count(1) from dfDec group by fare_amount_group order by fare_amount_group").collect()
	# with open("result/fare_group.txt","w") as f:
	# 	for i in fare_group:
	# 		f.write(str(i[0])+"\t"+str(i[1])+"\n")

	# tip_group = spark.sql("select tip_amount_group,count(1) from dfDec group by tip_amount_group order by tip_amount_group").collect()
	# with open("result/tip_group.txt","w") as f:
	# 	for i in tip_group:
	# 		f.write(str(i[0])+"\t"+str(i[1])+"\n")

	# location_avg_fare = spark.sql("select Zone,avg(fare_amount) from dfDec group by Zone order by Zone").collect()
	# with open("result/location_avg_fare.txt","w") as f:
	# 	for i in location_avg_fare:
	# 		f.write(str(i[0])+"\t"+str(i[1])+"\n")

	# location_avg_tips = spark.sql("select Zone,avg(tip_amount) from dfDec group by Zone order by Zone").collect()
	# with open("result/location_avg_tips.txt","w") as f:
	# 	for i in location_avg_tips:
	# 		f.write(str(i[0])+"\t"+str(i[1])+"\n")

	# hours_avg_tips = spark.sql("select pickup_hours,avg(tip_amount) from dfDec group by pickup_hours order by pickup_hours").collect()
	# with open("result/hours_avg_tips.txt","w") as f:
	# 	for i in hours_avg_tips:
	# 		f.write(str(i[0])+"\t"+str(i[1])+"\n")


	# hours_avg_fare = spark.sql("select pickup_hours,avg(fare_amount) from dfDec group by pickup_hours order by pickup_hours").collect()
	# with open("result/hours_avg_fare.txt","w") as f:
	# 	for i in hours_avg_fare:
	# 		f.write(str(i[0])+"\t"+str(i[1])+"\n")



	# hours_count =  spark.sql("select pickup_hours,avg(fare_amount) from dfDec group by pickup_hours order by pickup_hours")
	


	






   
