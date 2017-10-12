import gmaps
import pandas as pd
import numpy as np
import glob



gmaps.configure(api_key="AIzaSyBZha7DKqyZDkEBIdsMh1ESZzxXnkZLcYw")
# l = [pd.read_csv(filename) for filename in glob.glob('/Users/yuanyaozhang/Desktop/MassiveData/Final Project/Dataset/test/part-00001-8d3d3708-4003-4bc6-9293-54c1fe91a5bc.csv')]
l = [pd.read_csv(filename) for filename in glob.glob('/Users/yuanyaozhang/Desktop/MassiveData/Final Project/HeatMap/Friday/123.csv')]
myData = pd.concat(l, axis=0)




locations = myData[["latitude", "longitude"]]

marker_locations = [
    (-34.0, -59.166672),
    (-32.23333, -64.433327),
    (40.166672, 44.133331),
    (51.216671, 5.0833302),
    (51.333328, 4.25)
]
# fare = myData["tip_amount"]

m = gmaps.Map()

# heatmap_layer = gmaps.heatmap_layer(locations,weights=fare)
heatmap_layer = gmaps.heatmap_layer(locations)
layer = gmaps.marker_layer(marker_locations)
heatmap_layer.max_intensity = 1000
heatmap_layer.point_radius = 50
m.add_layer(heatmap_layer)
m.add_layer(layer)


m