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

start_value = int(start_value)
end_value = int(end_value)
period_seconds = int(period_seconds)
step = int(step)

c = start_value
while True:
    sleep(period_seconds)
    key = metric_name
    if c == start_value:
        value = start_value
        c += step
    elif c == end_value:
        value = end_value
        c = start_value
    else:
        value = c
        c += step
    package = {"value": value}
    print(f"{key}: {package}")
    producer.produce(TOPIC, key=key, value=json.dumps(value))
    producer.flush()
