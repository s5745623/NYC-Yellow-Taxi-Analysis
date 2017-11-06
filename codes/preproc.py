# For pyspark command line use

def getNum(time, day_of_week):
    b = 48
    minutes_per_bin = int((24 / float(b)) * 60)
    time_list = [int(t) for t in time.split(':')]
    num_minutes = time_list[0] * 60 + time_list[1]
    time_bin = num_minutes / minutes_per_bin
    time_bin = float(0.5*math.floor(2.0*time_bin))
    hour_bin = int(num_minutes / 60)
    min_bin = int((time_bin * minutes_per_bin) % 60)
    hour_str = str(hour_bin) if hour_bin / 10 > 0 else "0" + str(hour_bin)
    min_str = str(min_bin) if min_bin / 10 > 0 else "0" + str(min_bin)
    time_cat = hour_str + ":" + min_str
    time_num = (hour_bin*60 + min_bin + minutes_per_bin / 2.0)/(60*24)
    time_cos = math.cos(time_num * 2 * math.pi)
    time_sin = math.sin(time_num * 2 * math.pi)
    day_num = (day_of_week + time_num)/7.0
    day_cos = math.cos(day_num * 2 * math.pi)
    day_sin = math.sin(day_num * 2 * math.pi)
    return (time_cat, time_num, time_cos, time_sin, day_num, day_cos, day_sin)


def lineExtractor(line):
    import geohash     
    time = line['time']
    day_of_week = int(line['day_of_week'])
    lst = getNum(time, day_of_week)
    g = 7                                                                                                 
    location = geohash.encode(float(line['latitude']),float(line['longitude']),g)                          
    new_row = Row(time_cat=lst[0],time_num=lst[1],time_cos=lst[2],time_sin=lst[3],day_cat=line['day_cat'],day_num=lst[4],day_cos=lst[5],day_sin=lst[6],weekend=line['weekend'],geohash=location,latitude=float(line['latitude']),longitude=float(line['longitude'])) 
    return new_row

df = spark.read.format('com.databricks.spark.csv').options(header='true').load('combined*.csv')

rddMon = df.rdd

SatSelected = rddSat.map(lambda line: Row(time=line['tpep_pickup_datetime'][11:16],day_cat='Saturday',day_of_week=5,weekend=1,latitude=line['pickup_latitude'],longitude=line['pickup_longitude']))

Monday = SatSelected.map(lambda line: lineExtractor(line)).filter(lambda row: row!=None).map(lambda row: (row,1)).reduceByKey(lambda a,b: a + b).map(lambda line: Row(day_cat=line[0]['day_cat'],day_cos=line[0]['day_cos'],day_num=line[0]['day_num'],day_sin=line[0]['day_sin'],geohash=line[0]['geohash'],time_cat=line[0]['time_cat'],time_cos=line[0]['time_cos'],time_num=line[0]['time_num'],time_sin=line[0]['time_sin'],weekend=line[0]['weekend'],pickups=line[1]))
                                                   