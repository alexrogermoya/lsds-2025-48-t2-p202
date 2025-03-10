from confluent_kafka import Producer
import json
import uuid
from time import sleep
from datetime import datetime
from random import choice
import sys

_, metric_name, low_value, spike_value, period_seconds, frequency = sys.argv


TOPIC = "metrics"
PRODUCER_CONFIG = {
    "bootstrap.servers": "localhost:19092",
    "client.id": f"metrics-producer-{uuid.uuid4()}",
}

producer = Producer(PRODUCER_CONFIG)

frequency = int(frequency)
period_seconds = int(period_seconds)
low_value = int(low_value)
spike_value = int(spike_value)

c = frequency

while True:
    sleep(period_seconds)
    key = metric_name
    if c == frequency:
        value = spike_value
        c -= 1
    elif c == 1:
        value = low_value
        c = frequency
    else:
        value = low_value
        c -= 1
    package = {"metric": key, "value": value}
    print(f"{key}: {package}")
    producer.produce(TOPIC, key=key, value=json.dumps(package))
    producer.flush()
