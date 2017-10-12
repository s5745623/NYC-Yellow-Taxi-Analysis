import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.cluster import DBSCAN
from sklearn import decomposition
from sklearn.preprocessing import StandardScaler
from scipy.cluster.hierarchy import dendrogram, linkage

from scipy.cluster.hierarchy import cophenet
from scipy.spatial.distance import pdist
import os
import glob

from bokeh.io import output_file, show
from bokeh.plotting import figure

from bokeh.models import (
  GMapPlot, GMapOptions, ColumnDataSource, Circle, DataRange1d, PanTool, WheelZoomTool, BoxSelectTool, HoverTool
)

# import sys,os,datetime,re,operator
# from pyspark import SparkContext, SparkConf
# from pyspark.sql import SparkSession
# from urllib.request import urlopen
# from pyspark.sql import SparkSession, Row, SQLContext
# from pyspark.sql.functions import udf,lit
# from pyspark.sql.types import *
# from datetime import datetime





# spark = SparkSession.builder.appName("data").getOrCreate()
# sc    = spark.sparkContext
# myData = spark.read.format('com.databricks.spark.csv').options(header='true').load("/Users/yuanyaozhang/Desktop/MassiveData/Final Project/Dataset/test")

########################################################################

l = [pd.read_csv(filename) for filename in glob.glob('/Users/yuanyaozhang/Desktop/MassiveData/Final Project/Dataset/test/*.csv')]
myData = pd.concat(l, axis=0)

# filelist = os.listdir('/Users/yuanyaozhang/Desktop/MassiveData/Final Project/Dataset/test/*') 
# df_list = [pd.read_table(file) for file in filelist]
# myData = pd.concat(df_list)

# dataFile = open('/Users/yuanyaozhang/Desktop/MassiveData/Final Project/Dataset/result/*.csv','r')
# myData = pd.read_csv('/Users/yuanyaozhang/Desktop/MassiveData/Final Project/Dataset/test/*.csv', sep=',', encoding='latin1')


######################################################################### KMEANS Cluster


API_KEY="AIzaSyBZha7DKqyZDkEBIdsMh1ESZzxXnkZLcYw"
Y=myData['dropoff_latitude']   #assigning latitude to X
X=myData['dropoff_longitude']   #assigning longitude to Y


  #dropping nan in Y
myDataFrame = pd.concat([X, Y], axis=1)   #putting X and Y together
myDataFrame = myDataFrame.dropna()
myDataFrame = myDataFrame.as_matrix()

k=5     # the number of cluster you want to see

kmeans = KMeans(n_clusters=k)      #initializing kmeans with k clusters
kmeans.fit(myDataFrame)         

labels=kmeans.labels_           
centroids=kmeans.cluster_centers_    #centroids
lat =[]
lon = []
for i in centroids:
    print(str(i[1])+',\t'+str(i[0]))
    lat.append(i[1])
    lon.append(i[0])
mydict= dict(latitude = lat, longitude = lon)


map_options = GMapOptions(lat=40.687611, lng=-73.893925, map_type="roadmap", zoom=11,)
plot = GMapPlot(
    x_range=DataRange1d(), y_range=DataRange1d(), map_options=map_options, api_key=API_KEY
)
plot.title.text = "Kmeans"

source = ColumnDataSource(
    data=mydict
)


circle = Circle(y="latitude", x="longitude", size=15, fill_color="blue", fill_alpha=0.8, line_color=None)
plot.add_glyph(source, circle)

plot.add_tools(PanTool(), WheelZoomTool(), BoxSelectTool(), HoverTool(tooltips=[ ("latitude,longitude", "(@latitude, @longitude)")]))
    
output_file("kmeans.html")
show(plot)

for i in range(k):
    # select only data observations with cluster label == i
    ds = myDataFrame[np.where(labels==i)]
    # plot the data observations
    plt.plot(ds[:,0],ds[:,1],'o')
    # plot the centroids
    lines = plt.plot(centroids[i,0],centroids[i,1],'kx')
    # make the centroid x's bigger
    plt.setp(lines,ms=15.0)
    plt.setp(lines,mew=2.0)
plt.ylabel('latitude'),plt.xlabel('longitude')
plt.title("KMeans")
plt.show()