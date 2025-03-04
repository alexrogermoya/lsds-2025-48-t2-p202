from confluent_kafka import Producer
import json
import uuid
from time import sleep
from datetime import datetime
from random import choice
import sys

_, metric_name, metric_value, period_seconds = sys.argv

TOPIC = "metrics"
PRODUCER_CONFIG = {
    "bootstrap.servers": "localhost:19092",
    "client.id": f"metrics-producer-{uuid.uuid4()}",
}

producer = Producer(PRODUCER_CONFIG)
while True:
    sleep(int(period_seconds))
    key = metric_name
    value = {"value": int(metric_value)}
    print(f"{key}: {value}")
    producer.produce(TOPIC, key=key, value=json.dumps(value))
    producer.flush()
