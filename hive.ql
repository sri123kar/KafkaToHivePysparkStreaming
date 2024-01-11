use database dl_dev;

CREATE External Table kafka_data (
card_id string, 
amount int, 
transaction_dt string)
stored as parquet
location '/user/hive/warehouse/dl_dev/kafka_data/' ;