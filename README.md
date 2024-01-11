Assumptions :
Data Structure :
card_id, amount, transaction_dt

Example: JSON data looks like:
[ {'123413132', 345, '2021-01-20'}, 
{'1234454532', 375, '2021-01-21'}
{'12342424132', 845, '2021-01-22'}
{'1234243132', 3405, '2021-01-23'} ] 


Prequitics: 
Make sure kafka is up and running and accessible
Update the Brokers address in code 
brokers = "broker1.test.corp.com:9092,broker2.test.corp.com:9092,broker3.test.corp.com:9092"

### First push data to kafka
It will read data.csv and push data to kafka for further processing
python kafka_stream.py 


### Spark Structure Streaming 
Command to Execute the Spark Structure Streaming code:

spark-submit  --master local --jars spark-sql-kafka-0-10_2.11-2.3.0.jar  driver.py

It will Streaming data from kafka to Hive External table "# KafkaToHivePysparkStreaming" 
