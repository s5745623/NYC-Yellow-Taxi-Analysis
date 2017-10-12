# run on Apache SparkSQL, by
#
# loading the dataset into a table and
#
# computing the result with an SQL query


import json
import urllib

class Splitter():

    def __init__(self,line):
        import datetime
        self.id = line[1]
        self.datetime_receive = datetime.datetime.strptime(line[8],"%Y-%m-%dT%H:%M:%S") # 'The date the CFPB received the complaint'
        self.datetime_sent = datetime.datetime.strptime(line[21],"%Y-%m-%dT%H:%M:%S")

    def Row(self):
        from pyspark.sql import Row
        return Row(id = self.id,datetime_receive = self.datetime_receive,datetime_sent = self.datetime_sent)


if __name__=="__main__":
    from pyspark.sql import SparkSession
    from pyspark import SparkContext, SparkConf

    spark = SparkSession.builder.appName("rows").getOrCreate()
    sc = spark.sparkContext  # get the context

    data = json.loads(urllib.request.urlopen("http://data.consumerfinance.gov/api/views/7zpz-7ury/rows.json").read().decode('utf-8'))

    events = sc.parallelize(data['data'])

    #events = data['data']

    rows = events.map(lambda l: Splitter(l).Row()).cache()
    schemaDF = spark.createDataFrame(rows)   
    schemaDF.createOrReplaceTempView("complaints")

    # query_result = spark.sql("select count(*) from quazyilx").collect()
    # print(query_result)

    print("rows: {}".format(spark.sql("SELECT SUBSTR(datetime_receive,1,4),count(id) as number FROM complaints GROUP BY SUBSTR(datetime_receive,1,4) order by SUBSTR(datetime_receive,1,4)").collect()))



