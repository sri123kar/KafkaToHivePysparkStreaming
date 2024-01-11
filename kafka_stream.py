import json
from time import sleep
from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers=['broker1.test.corp.com:9092'])

# reading data from file
file_data = open("data.csv", "r")
for line in file_data:
	words = line.replace("\n","").split(",")
	data = json.dumps({"card_id":words[0],"amount":words[1],"transaction_dt":words[2]}).encode('utf-8')
	producer.send('Test-Partition', value=data)
	sleep(5)