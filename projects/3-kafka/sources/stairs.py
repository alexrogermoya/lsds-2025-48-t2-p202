from confluent_kafka import Producer
import json
import uuid
from time import sleep
from datetime import datetime
from random import choice
import sys

_, metric_name, start_value, end_value, step, period_seconds = sys.argv


TOPIC = "metrics"
PRODUCER_CONFIG = {
    "bootstrap.servers": "localhost:19092,localhost:29092,localhost:39092",
    "client.id": f"metrics-producer-{uuid.uuid4()}",
}

producer = Producer(PRODUCER_CONFIG)

c = int(start_value)
step = int(step)
while True:
    sleep(int(period_seconds))
    key = metric_name
    if c == int(start_value):
        value = int(start_value)
        c += step
    elif c == int(end_value):
        value = int(end_value)
        c = int(start_value)
    else:
        value = c
        c += step
    package = {"value": value}
    print(f"{key}: {package}")
    producer.produce(TOPIC, key=key, value=json.dumps(value))
    producer.flush()
