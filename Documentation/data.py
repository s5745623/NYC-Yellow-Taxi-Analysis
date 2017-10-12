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
		return "Wen"
	if date_num==3:
		return "Tru"
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
    dfDec2 = spark.read.format('com.databricks.spark.csv').options(header='true').load("/Users/chenyujing/Downloads/dataset/*.csv")
    dfDec3 = spark.read.format('com.databricks.spark.csv').options(header='true').load("/Users/chenyujing/Downloads/taxi+_zone_lookup.csv")

    cols= list(set(dfDec.columns + dfDec2.columns))
    print (cols)
    dfDec = dfDec.withColumn("PULocationID",  lit("9999")).withColumn("DOLocationID",lit("9999")).select(cols)
    dfDec2 = dfDec2.withColumn("pickup_longitude"  ,lit("9999")).withColumn("pickup_latitude"  ,lit("9999")).withColumn("dropoff_longitude"  ,lit("9999")).withColumn("dropoff_latitude"  ,lit("9999")).select(cols)

    # dfDec2 = spark.read.format('com.databricks.spark.csv').options(header='true').load("Untitled.csv")
    
    dfDec = dfDec.unionAll(dfDec2)

    dfDec = dfDec.join(dfDec3, dfDec.PULocationID == dfDec3.LocationID, 'left')


    # dfDec.write.csv("/Users/chenyujing/Downloads/dataset/result.csv")
    # dfDec.show()

    tofloat = dfDec.withColumn("fare_amount",dfDec["fare_amount"].cast("double")).withColumn("tip_amount",dfDec["tip_amount"].cast("double")).withColumn("dropoff_latitude",dfDec["dropoff_latitude"].cast("double")).withColumn("dropoff_longitude",dfDec["dropoff_longitude"].cast("double")).withColumn("pickup_latitude",dfDec["pickup_latitude"].cast("double")).withColumn("pickup_longitude",dfDec["pickup_longitude"].cast("double"))

    tofloat = tofloat.filter("dropoff_latitude != 0 ").filter("dropoff_longitude != 0 " ).filter("pickup_latitude != 0 " ).filter("pickup_longitude != 0 ")


    udf_get_pickup_hour = udf(get_pickup_hour, StringType())
    tofloat = tofloat.withColumn("pickup_hours", udf_get_pickup_hour("tpep_pickup_datetime"))
    tofloat = tofloat.withColumn("dropoff_hours", udf_get_pickup_hour("tpep_dropoff_datetime"))

    udf_get_weekday = udf(get_weekday, StringType())
    tofloat = tofloat.withColumn("pickup_weekday", udf_get_weekday("tpep_pickup_datetime"))


    udf_get_if_weekday = udf(get_if_weekday, StringType())
    tofloat = tofloat.withColumn("if_weekday", udf_get_if_weekday("tpep_pickup_datetime"))


    udf_get_fare_amount = udf(get_fare_amount, StringType())
    tofloat = tofloat.withColumn("fare_amount_group", udf_get_fare_amount("fare_amount"))

    
    udf_get_tip_amount = udf(get_tip_amount, StringType())
    tofloat = tofloat.withColumn("tip_amount_group", udf_get_tip_amount("tip_amount"))


    udf_get_month = udf(get_month, StringType())
    tofloat = tofloat.withColumn("month", udf_get_month("tpep_pickup_datetime"))


    # tofloat.write.format("com.databricks.spark.csv").options(header='true').save("/Users/chenyujing/Downloads/dataset/result.csv")

    tofloat.cache()
    
    tofloat.createOrReplaceTempView("dfDec")





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
    
    month_num = spark.sql("select month,count(1) from dfDec group by month order by month").collect()
    with open("result/month_num.txt","w") as f:
        for i in month_num:
            f.write(str(i[0])+"\t"+str(i[1])+"\n")

    week_num = spark.sql("select pickup_weekday,count(1) from dfDec group by pickup_weekday").collect()
    with open("result/week_data.txt","w") as f:
    	for i in week_num:
    		f.write(str(i[0])+"\t"+str(i[1])+"\n")

    month_ave_fare = spark.sql("select month,avg(fare_amount) from dfDec group by month order by month").collect()
    with open("result/month_ave_fare.txt","w") as f:
    	for i in month_ave_fare:
    		f.write(str(i[0])+"\t"+str(i[1])+"\n")

    week_ave_fare = spark.sql("select pickup_weekday,avg(fare_amount) from dfDec group by pickup_weekday order by pickup_weekday").collect()
    with open("result/week_ave_fare.txt","w") as f:
    	for i in week_ave_fare:
    		f.write(str(i[0])+"\t"+str(i[1])+"\n")

    month_sum_fare = spark.sql("select month,sum(fare_amount) from dfDec group by month order by month").collect()
    with open("result/month_sum_fare.txt","w") as f:
    	for i in month_sum_fare:
    		f.write(str(i[0])+"\t"+str(i[1])+"\n")

    week_sum_fare = spark.sql("select pickup_weekday,sum(fare_amount) from dfDec group by pickup_weekday order by pickup_weekday").collect()
    with open("result/week_sum_fare.txt","w") as f:
    	for i in week_sum_fare:
    		f.write(str(i[0])+"\t"+str(i[1])+"\n")

    fare_group = spark.sql("select fare_amount_group,count(1) from dfDec group by fare_amount_group order by fare_amount_group").collect()
    with open("result/fare_group.txt","w") as f:
    	for i in fare_group:
    		f.write(str(i[0])+"\t"+str(i[1])+"\n")

    tip_group = spark.sql("select tip_amount_group,count(1) from dfDec group by tip_amount_group order by tip_amount_group").collect()
    with open("result/tip_group.txt","w") as f:
    	for i in tip_group:
    		f.write(str(i[0])+"\t"+str(i[1])+"\n")

    location_avg_fare = spark.sql("select Zone,avg(fare_amount) from dfDec group by Zone order by Zone").collect()
    with open("result/location_avg_fare.txt","w") as f:
    	for i in location_avg_fare:
    		f.write(str(i[0])+"\t"+str(i[1])+"\n")

    location_avg_tips = spark.sql("select Zone,avg(tip_amount) from dfDec group by Zone order by Zone").collect()
    with open("result/location_avg_tips.txt","w") as f:
    	for i in location_avg_tips:
    		f.write(str(i[0])+"\t"+str(i[1])+"\n")

    hours_avg_tips = spark.sql("select pickup_hours,avg(tip_amount) from dfDec group by pickup_hours order by pickup_hours").collect()
    with open("result/hours_avg_tips.txt","w") as f:
    	for i in hours_avg_tips:
    		f.write(str(i[0])+"\t"+str(i[1])+"\n")


    hours_avg_fare = spark.sql("select pickup_hours,avg(fare_amount) from dfDec group by pickup_hours order by pickup_hours").collect()
    with open("result/hours_avg_fare.txt","w") as f:
    	for i in hours_avg_fare:
    		f.write(str(i[0])+"\t"+str(i[1])+"\n")



    





   