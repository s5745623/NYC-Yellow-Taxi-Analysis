from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row, SQLContext
import os

def moneyGroup(amount):
    if 0<=amount<20:
        return '1'
    elif 20<=amount<40:
        return '2'
    elif 40<=amount<60:
        return '3'
    elif 60<=amount<80:
        return '4'
    elif 80<=amount<100:
        return '5'
    else:
        return '6'

def cleanData(infile, outfile):
    spark = SparkSession.builder.appName("NYC_Taxi").getOrCreate()
    sc    = spark.sparkContext
    sqlContext = SQLContext(sc)

    dfDec = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(infile)
    rddDec = dfDec.rdd

    taxiDec = rddDec.map(lambda line: Row(PULocationID=line['PULocationID'],DOLocationID=line['DOLocationID'],tpep_pickup_date=line['tpep_pickup_datetime'][0:10],fare=line['fare_amount'],tip=line['tip_amount'],distance=line['trip_distance']))

    taxiDec_clean = taxiDec.filter(lambda line: (float(line['fare'])>0 and float(line['tip'])>=0 ))

    taxiDec_group = taxiDec_clean.map( lambda line: Row(PULocationID=line['PULocationID'],DOLocationID=line['DOLocationID'],tpep_pickup_date=line['tpep_pickup_date'],fare_group=moneyGroup(float(line['fare'])/2),tip_group=moneyGroup(float(line['tip'])),distance=line['distance']))

    df = spark.createDataFrame(taxiDec_group)
    df.write.format('com.databricks.spark.csv').options(header='true').save(outfile)
    
    return
    
if __name__=="__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--infile",help="input file",default='yellow_tripdata_2015-12.csv')
    parser.add_argument("--outfile",help="output file",default='2015-12.csv')
    args = parser.parse_args()
    
    if os.path.exists(args.outfile):
        raise RuntimeError(args.outfile + " already exists. Please delete it.")
    
    cleanData(args.infile, args.outfile)
    
    print("{} -> {}".format(args.infile,args.outfile))


    

