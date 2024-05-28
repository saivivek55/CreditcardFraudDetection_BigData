#pymongo is used for importing the main stream 
from pymongo import MongoClient

#kafka is being used to consume message from kafka
from kafka import KafkaConsumer
import json

# MongoDB connection to establish connection between the server and
#mongoDB database.
client = MongoClient('localhost', 9092)
db = client['fraud_detection']
collection = db['flagged_transactions']

# Kafka consumer configuration
consumer = KafkaConsumer('flagged_transactions', 
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset='earliest',
                         enable_auto_commit=False,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Consume flagged transactions from Kafka and write to MongoDB
for msg in consumer:
    transaction = msg.value
    collection.insert_one(transaction)
    print("Transaction inserted into MongoDB:", transaction)

# Close MongoDB client
client.close()
