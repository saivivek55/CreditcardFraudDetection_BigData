#faker library used to generated dummy dataset of the credit card transactions
#kafka-python library used to produce the transactions to the Kafka topic

from faker import Faker
import random
import datetime
from kafka import KafkaProducer

# Initialize Faker
fake = Faker()

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Generate credit card transaction data
def generate_transaction():
    card_number = fake.credit_card_number()
    transaction_amount = round(random.uniform(1, 1000), 2)  # Random amount between 1 and 1000
    timestamp = fake.date_time_between(start_date='-1y', end_date='now')  # Random timestamp in the last year
    location = fake.city() + ', ' + fake.state()  # Random city and state
    merchant_category = fake.random_element(elements=('Retail', 'Restaurant', 'Travel', 'Online', 'Entertainment'))  # Random merchant category
    currency = fake.currency_code()  # Random currency code
    device_type = fake.random_element(elements=('Desktop', 'Mobile', 'Tablet'))  # Random device type

    # Return transaction data as dictionary
    return {
        'card_number': card_number,
        'transaction_amount': transaction_amount,
        'timestamp': timestamp,
        'location': location,
        'merchant_category': merchant_category,
        'currency': currency,
        'device_type': device_type
    }

# Generate and produce transactions to Kafka topic
topic = 'credit_card_transactions'
num_transactions = 1000
for _ in range(num_transactions):
    transaction = generate_transaction()
    producer.send(topic, str(transaction).encode('utf-8'))

# Flush and close the producer
producer.flush()
producer.close()
