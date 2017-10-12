
# Downloads the dataset into a Python string.

import requests
import json
import datetime

url = "http://data.consumerfinance.gov/api/views/7zpz-7ury/rows.json"
response = requests.get(url).content.decode()

# Uses the json package to turn the JSON string into Python dictionary. 

data = json.loads(response)

# Compute the number of compaints for each year.

# 'The date the CFPB received the complaint' - column 8

lis = []

for complain in data['data']:
    lis.append(datetime.datetime.strptime(complain[8],"%Y-%m-%dT%H:%M:%S").year)

counts = [(y,lis.count(y)) for y in sorted(set(lis))]

with open("complaints.txt", "w") as f:
	for complain in counts:
	    print("%d %d"%(complain[0],complain[1]))
	    f.write("%d %d\n"%(complain[0],complain[1]))


# Extra Credit Option #1: 
# How many complaints were there against PayPal Holdings, Inc? Put your answer in paypal.txt

paypal = [complain[15]=='PayPal Holdings, Inc.' for complain in data['data']]
with open("paypal.txt", "w") as f:
    f.write('Complaints against PayPal Holdings, Inc. : ' + str(sum(paypal)))


