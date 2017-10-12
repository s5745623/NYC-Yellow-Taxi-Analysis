import boto3
import json
from urllib.request import urlopen
import collections
from boto3.dynamodb.conditions import Key,Attr

if __name__=="__main__":
    dynamodb = boto3.resource('dynamodb',region_name="us-east-1",endpoint_url="http://localhost:8000")
    # dynamodb = boto3.resource('dynamodb')
    #dynamodb = boto3.resource('dynamodb',region_name="us-east-1")
    data = json.loads(urlopen("http://data.consumerfinance.gov/api/views/7zpz-7ury/rows.json").read().decode('utf-8'))

    # The attributes are too many, so I just create the table with two attributes, it's enough to calculate the results
    
    table = dynamodb.create_table(
    	TableName = 'yc704',
    	KeySchema = [
    	{
    	    "AttributeName": 'date_year',
    	    "KeyType": 'HASH'
    	},
            {
            "AttributeName": 'Num',
            "KeyType": 'RANGE'
            }
    	],
    	AttributeDefinitions = [
    	{
    	    "AttributeName" : 'date_year',
    	    "AttributeType" : 'S'
    	},
            {
                 "AttributeName" : 'Num',
                 "AttributeType" : 'N'
            }
    	],
    	ProvisionedThroughput={
    	    'ReadCapacityUnits': 100,
    	    'WriteCapacityUnits': 100,
    	}
    	
    )

    table.wait_until_exists()
    # table.meta.client.get_waiter('table_exists').wait(TableName='yc704')
    
    year = []
    for element in data['data']:
        # [0:4] is year
        if element[8][0:4] not in year:
            year.append(element[8][0:4])
        table.put_item(
        	Item ={
        	    'date_year' : element[8][0:4],
                "Num" : element[0]
        	}
        )

    year = sorted(year)

    #  calculate and output
    with open("complaints_dd.txt","w") as f :
        for element in year:
            count = table.query(Select = 'COUNT',KeyConditionExpression = Key('date_year').eq(element))
            print (str(element) +'\t'+str(count['Count']))
            f.write(str(element) +'\t'+str(count['Count'])+'\n')
