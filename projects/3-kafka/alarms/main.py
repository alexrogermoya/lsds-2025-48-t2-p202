from fastapi import FastAPI
from kafka import KafkaConsumer
import json
import threading

# Initialize FastAPI app
app = FastAPI()

# Dictionary to hold rules 
rules_materialized_view = {}

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

# A simple route for testing
@app.get("/")
def read_root():
    return {"message": "Alarms Service is running", "rules": rules_materialized_view}