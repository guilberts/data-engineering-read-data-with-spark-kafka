from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, LongType, DoubleType
import json
import time
import random
from threading import Thread
from confluent_kafka import Producer

# Simple sample showing how to use pySpark with Apache Kafka


# Setup Spark
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.driver.host", "localhost")  \
    .getOrCreate()

# Data Schema 
schema = StructType() \
    .add("id", LongType()) \
    .add("text", StringType()) \
    .add("timestamp", DoubleType())

# Setup Kafka producer
conf = {'bootstrap.servers': 'localhost:29092'}
producer = Producer(conf)

def generate_test_message():
    return {
        "id": random.randint(1, 1000),
        "text": f"Test message {random.randint(1, 100)}",
        "timestamp": time.time()
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"Error can't send message: {err}")
    else:
        print(f'Success ! Message sent to {msg.topic()} [{msg.partition()}]')

def produce_messages(topic):
    while True:
        message = generate_test_message()
        producer.produce(topic, json.dumps(message).encode('utf-8'), callback=delivery_report)
        producer.poll(0) #callback to make sure message was received
        time.sleep(0.1)

def read_stream(topic):
    # read Kafka topic
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:29092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Convert the value from binary to string and then apply JSON schema
    df = df.select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), schema).alias("data"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp")
    ).select("key", "data.*", "topic", "partition", "offset", "timestamp")
    
    return df

def write_console(df):
    # Write the stream to the console for debugging
    query = df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    return query

if __name__ == "__main__":
    topic = "test-topic"

    # Start producer in a separeted thread
    producer_thread = Thread(target=produce_messages, args=(topic,))
    producer_thread.daemon = True
    producer_thread.start()

    # Read stream
    df = read_stream(topic)
   
    # Write into console
    query = write_console(df)
    
    # Waiting query streaming
    query.awaitTermination()