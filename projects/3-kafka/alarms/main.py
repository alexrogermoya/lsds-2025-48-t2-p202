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
        bootstrap_servers='kafka-cluster-kafka-1-1:9092',
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
        bootstrap_servers='kafka-cluster-kafka-1-1:9092',  
        group_id='alarms-service-metrics',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    for message in consumer:
        metric = message.value
        print(f"Received metric: {metric}")
        for rule_id, rule in rules_materialized_view.items():
            if metric['name'] == rule['metric']:
                if rule['condition'] == 'greater_than' and metric['value'] > rule['threshold']:
                    print(f"Rule triggered! Metric {metric['name']} exceeded the threshold.")
                    send_alarm_to_discord(rule, metric) # Send an alarm when the rule is triggered

# Start the metrics consumer in a background thread
def start_metrics_consumer():
    metrics_consumer_thread = threading.Thread(target=consume_metrics)
    metrics_consumer_thread.daemon = True
    metrics_consumer_thread.start()
start_metrics_consumer()

# -------------------------------------------
# L6Q2: Sending Alarms to Discord
# -------------------------------------------

# Function to send alarm to Discord when a rule is triggered
def send_alarm_to_discord(rule, metric):
    discord_webhook_url = rule.get('discord_url') 
    if discord_webhook_url:
        alarm_message = {
            "content": f"Alarm triggered for {metric['name']}",
            "embeds": [
                {
                    "title": "Alarm triggered",
                    "description": f"{metric['name']} value {metric['value']} exceeded threshold {rule['threshold']}",
                    "color": 16711680,  
                    "fields": [
                        {"name": "Rule ID", "value": rule['id']},
                        {"name": "Metric Name", "value": metric['name']},
                        {"name": "Metric Value", "value": metric['value']},
                        {"name": "Rule Threshold", "value": rule['threshold']}
                    ]
                }
            ]
        }
        response = requests.post(discord_webhook_url, json=alarm_message)
        if response.status_code == 204:
            print(f"Alarm sent to Discord for rule {rule['id']}")
        else:
            print(f"Failed to send alarm for rule {rule['id']}")

