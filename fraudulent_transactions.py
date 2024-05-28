from datetime import datetime
from confluent_kafka import Consumer, Producer, KafkaError
import json

# Initialize Kafka Consumer
consumer_config = {
    'bootstrap.servers': 'XXXX',  
    'group.id': 'XXXX', 
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'XXXX',  # API key
    'sasl.password': 'XXXX'  # API secret
}

consumer = Consumer(consumer_config)
consumer.subscribe(['credit_card_transaction'])  # consume topic

# Initialize Kafka Producer
producer_config = {
    'bootstrap.servers': 'XXXX',  
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'XXXX',  # Kafka API key
    'sasl.password': 'XXXX'  # Kafka API secret
}

producer = Producer(**producer_config)

def delivery_report(err, msg):
    if err:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def is_fraud(transaction):
    transaction_time = datetime.fromisoformat(transaction['timestamp']).time()
    # Define the fraudulent time range (10 PM to 6 AM)
    start_fraud_time = datetime.strptime("22:00:00", "%H:%M:%S").time()
    end_fraud_time = datetime.strptime("06:00:00", "%H:%M:%S").time()

    # Check all conditions in one line
    return (
        transaction['amount'] > 100 and
       # transaction['location']['city'] == "West Jamie" and
        transaction['device_type'] == "mobile" and
        (transaction_time >= start_fraud_time or transaction_time <= end_fraud_time)
    )

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f'Consumer error: {msg.error()}')
            continue

        transaction = json.loads(msg.value().decode('utf-8'))
        print(f'Received transaction: {transaction}') 

        if is_fraud(transaction):
            print(f'Suspicious transaction detected: {transaction}')
            producer.produce('flagged_transactions', key=str(transaction['timestamp']).encode('utf-8'), value=json.dumps(transaction).encode('utf-8'), callback=delivery_report)
            producer.poll(0) 
finally:
    # Clean up on exit
    consumer.close()
    producer.flush()
    producer.close()