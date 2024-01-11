from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json

spark = SparkSession.builder.appName('KafkaToHiveETL') \
          .config("spark.sql.warehouse.dir", "/tmp/naga") \
          .config("hive.exec.dynamic.partition.mode", "nonstrict") \
          .enableHiveSupport() \
          .getOrCreate()

# Params 
topic_name = "Test-Partition"
brokers = "broker1.test.corp.com:9092,broker2.test.corp.com:9092,broker3.test.corp.com:9092"

# ASSUMED : schema for the kafka messages
schema = StructType([
    StructField("card_id", StringType(), True), 
    StructField("amount", FloatType(), True), 
    StructField("transaction_dt", StringType(), True)
    ])

# start the kafka streaming 
df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", brokers) \
  .option("subscribe", topic_name) \
  .option("startingOffsets","earliest") \
  .load()

# extract all fields from kafka message 
df = df.selectExpr('CAST(value AS STRING)') \
    .select(from_json('value', schema).alias("value" )) \
    .select("value.card_id","value.amount","value.transaction_dt")


# write stream on console
df.writeStream
.format("parquet")
.option("path","/user/hive/warehouse/dl_dev/kafka_data/")
.option("checkpointLocation","/checkpoint_path")
.outputMode("append")
.start()


