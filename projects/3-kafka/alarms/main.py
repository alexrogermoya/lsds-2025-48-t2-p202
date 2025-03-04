from fastapi import FastAPI
from kafka import KafkaConsumer
import threading
import json
import requests

# Initialize FastAPI app
app = FastAPI()

# Dictionary to hold rules 
rules_materialized_view = {}

# -------------------------------------------
# L6Q0: Create the Materialized View of Rules
# -------------------------------------------

# Kafka consumer to consume rules topic
def consume_rules():
    consumer = KafkaConsumer(
        'rules',
        bootstrap_servers='kafka-1:9092',
        group_id='alarms-service',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    for message in consumer:
        rule_id = message.key.decode('utf-8')
        new_rule = message.value
        if new_rule == "":
            if rule_id in rules_materialized_view:
                del rules_materialized_view[rule_id]
        else:
            rules_materialized_view[rule_id] = new_rule
        print("Updated rules:", rules_materialized_view)

# Start the consumer in a background thread
def start_background_thread():
    consumer_thread = threading.Thread(target=consume_rules)
    consumer_thread.daemon = True
    consumer_thread.start()
start_background_thread()

# -------------------------------------------
# L6Q1: Consume Metrics and Match Rules
# -------------------------------------------

# Kafka consumer to consume metrics topic and check against the materialized view of rules
def consume_metrics():
    consumer = KafkaConsumer(
        'metrics',
        bootstrap_servers='kafka-1:9092',
        group_id='alarms-service-metrics',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    for message in consumer:
        metric = message.value
        print(f"Received metric: {metric}")
        for rule_id, rule in rules_materialized_view.items():
            if metric['name'] == rule['metric'] and metric['value'] > rule['threshold']:
                print(f"Rule triggered! Metric {metric['name']} exceeded the threshold.")

# Start the metrics consumer in a background thread
def start_metrics_consumer():
    metrics_consumer_thread = threading.Thread(target=consume_metrics)
    metrics_consumer_thread.daemon = True
    metrics_consumer_thread.start()
start_metrics_consumer()