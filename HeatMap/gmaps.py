import gmaps
import pandas as pd
import numpy as np
import glob



gmaps.configure(api_key="AIzaSyBZha7DKqyZDkEBIdsMh1ESZzxXnkZLcYw")
# l = [pd.read_csv(filename) for filename in glob.glob('/Users/yuanyaozhang/Desktop/MassiveData/Final Project/Dataset/test/part-00001-8d3d3708-4003-4bc6-9293-54c1fe91a5bc.csv')]
l = [pd.read_csv(filename) for filename in glob.glob('/Users/yuanyaozhang/Desktop/MassiveData/Final Project/Dataset/HeatMap/2016-mon/combine.csv')]
myData = pd.concat(l, axis=0)




locations = myData[["pickup_latitude", "pickup_longitude"]]
# fare = myData["tip_amount"]

m = gmaps.Map()

# heatmap_layer = gmaps.heatmap_layer(locations,weights=fare)
heatmap_layer = gmaps.heatmap_layer(locations)
heatmap_layer.max_intensity = 1000
heatmap_layer.point_radius = 0.5
m.add_layer(heatmap_layer)


m