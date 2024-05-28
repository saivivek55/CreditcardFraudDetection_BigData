from confluent_kafka import Consumer, KafkaError
import json
from pymongo import MongoClient

# MongoDB setup
client = MongoClient('XXXX')  # Replace with your actual MongoDB connection string
db = client['fraud_detection']
collection = db['flagged_transactions']

# Kafka Consumer setup
consumer_config = {
    'bootstrap.servers': 'XXXX',  
    'group.id': 'mongo_writer_group',  
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'XXXX',  
    'sasl.password': 'XXXX',  
    'enable.auto.commit': True,
}

consumer = Consumer(**consumer_config)
consumer.subscribe(['flagged_transactions'])  

try:
    while True:
        msg = consumer.poll(1.0)  
        if msg is None:
            continue  
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue  
            else:
                print(f'Consumer error: {msg.error()}')
                continue

        
        transaction = json.loads(msg.value().decode('utf-8'))
        print(transaction) 
        collection.insert_one(transaction)  

finally:
    # Clean up on exit
    consumer.close()