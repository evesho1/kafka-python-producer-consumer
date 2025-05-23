import csv
from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')

with open('Kafkatest.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        message = str(row).encode('utf-8')
        producer.send('people_topic', message)
        print(f"Sent: {message}")
        time.sleep(1)
producer.flush()
