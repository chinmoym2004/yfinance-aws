import json
import boto3
import sys
import yfinance as yf

import time
import random
import datetime
import uuid
import json


# Your goal is to get per-hour stock price data for a time range for the ten stocks specified in the doc. 
# Further, you should call the static info api for the stocks to get their current 52WeekLow and 52WeekLow values.
# You should craft individual data records with information about the stockid, price, price timestamp, 52WeekHigh and 52WeekLow values and push them individually on the Kinesis stream

kinesis = boto3.client('kinesis', region_name = "us-east-1") #Modify this line of code according to your requirement.

today = datetime.date.today()
yesterday = datetime.date.today() - datetime.timedelta(2)

## Add code to pull the data for the stocks specified in the doc
stocksource = ["MSFT", "MVIS", "GOOG", "SPOT", "INO", "OCGN", "ABML", "RLLCF", "JNJ", "PSFE"]

def dict_to_binary(the_dict):
    str = json.dumps(the_dict)
    binary = ' '.join(format(ord(letter), 'b') for letter in str)
    return binary


df_list = dict()
for eachsource in stocksource:
	print("\n ****** PULLING DATA FOR : ",eachsource," ******\n")
	# Example of pulling the data between 2 dates from yfinance API

	data = yf.download(tickers=eachsource,start= yesterday, end= today, interval = '1h' )
	data.reset_index(inplace=True)
	data['Date'] = data['index'].dt.strftime('%Y-%m-%d %H:%I:%S')
	data = data.to_dict(orient='records');

	## Add additional code to call 'info' API to get 52WeekHigh and 52WeekLow refering this this link - https://pypi.org/project/yfinance/
	ticker = yf.Ticker(eachsource)
	info = ticker.info
	#print("52WeekHigh: ",info['fiftyTwoWeekHigh'])
	#print("52WeekLow: ",info['fiftyTwoWeekLow'])

	## Add your code here to push data records to Kinesis stream.
	## Prepare the daata
	for eachrow in data:
		df_list["StockID"]=str(uuid.uuid4());
		df_list["timestamp"]=eachrow['Date'];
		df_list["price"]=eachrow['Open'];
		df_list["52WeekHigh"]=info['fiftyTwoWeekHigh'];
		df_list["52WeekLow"]=info['fiftyTwoWeekLow'];
		print("Inserting..\n")
		print(df_list)
		# push to Kinesis
		response = kinesis.put_record(
		    StreamName='StockDataStream',
		    Data=dict_to_binary(df_list),
		    PartitionKey='StockID',
		)
		print("\nKinesis Response ..\n")
		print(response)
		print("\n\n")


