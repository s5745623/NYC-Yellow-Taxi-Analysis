
import pandas as pd
import numpy as np
import glob




# l = [pd.read_csv(filename) for filename in glob.glob('/Users/yuanyaozhang/Desktop/MassiveData/Final Project/Dataset/test/part-00001-8d3d3708-4003-4bc6-9293-54c1fe91a5bc.csv')]
l = [pd.read_csv(filename) for filename in glob.glob('/Users/yuanyaozhang/Desktop/MassiveData/Final Project/Dataset/HeatMap/2016-wed/combine.csv')]
myData = pd.concat(l, axis=0)




clean = myData[["pickup_latitude", "pickup_longitude","tip_amount","fare_amount"]]
clean.to_csv('clean.csv')



