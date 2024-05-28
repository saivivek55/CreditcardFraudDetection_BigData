import json
import random
import time
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

conf = {
    'bootstrap.servers': 'XXXX',  
    'client.id': 'credit_card_producer',
    'security.protocol': 'SASL_SSL',  
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'XXXX',  #API key
    'sasl.password': 'XXXX'  # API secret key
}

producer = Producer(conf)

def generate_random_transaction():
    card_number = fake.credit_card_number()
    amount = round(random.uniform(1, 1000), 2)
    timestamp = fake.date_time().isoformat() 
    location = {
        "city": fake.city(),
        "state": fake.state_abbr()
    }
    merchant = fake.company()
    currency= fake.currency_code()
    device_type= fake.random_element(["mobile", "tablet", "desktop"])

    return {
        "card_number": card_number,
        "amount": amount,
        "timestamp": timestamp,
        "location": location,
        "merchant": merchant,
        "currency": currency,
        "device_type": device_type
    }

while True:
    transaction = generate_random_transaction()
    producer.produce("credit_card_transaction", json.dumps(transaction).encode("utf-8"))
    producer.flush()
    print(json.dumps(transaction, indent=2)) 
    time.sleep(1)
