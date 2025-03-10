from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from pydantic import BaseModel
import os
import json
import uuid

# Initialize FastAPI app
app = FastAPI()

rules_db = {}

# Get broker address from environment variable
KAFKA_BROKER = os.getenv("BROKER", "kafka-1:9092")

print(f"Connecting to Kafka at {KAFKA_BROKER}...")

# Configure Kafka producer with error handling
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8") if v is not None else None,
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
    )

    print("✅ Connected to Kafka successfully!")
except Exception as e:
    print(f"❌ Error connecting to Kafka: {e}")
    producer = None

# Define Rule Model
class RuleRequest(BaseModel):
    metric: str
    condition: str
    threshold: float
    discord_webhook_url: str = None

@app.post("/rules")
def create_rule(rule: RuleRequest):
    global rules_db

    rule_id = str(uuid.uuid4())
    rule_data = {
        "id": rule_id,
        "metric": rule.metric,
        "condition": rule.condition,
        "threshold": rule.threshold,
        "discord_webhook_url": rule.discord_webhook_url,
    }

    rules_db[rule_id] = rule_data
    print("Current rules_db:", rules_db)

    if producer:
        try:
            producer.send("rules", key=rule_id.encode("utf-8"), value=rule_data)
            producer.flush()
            print(f"✅ Rule {rule_id} sent to Kafka")
        except Exception as e:
            print(f"❌ Error sending rule to Kafka: {e}")

    return {"message": "Rule created", "rule": rule_data}

@app.delete("/rules/{rule_id}")
def delete_rule(rule_id: str):
    global rules_db

    if rule_id not in rules_db:
        raise HTTPException(status_code=404, detail="Rule not found")

    del rules_db[rule_id]

    if producer:
        try:
            producer.send("rules", key=rule_id.encode("utf-8"), value=None)
            producer.flush()
            print(f"✅ Rule {rule_id} deleted and sent to Kafka")
        except Exception as e:
            print(f"❌ Error deleting rule from Kafka: {e}")

    return {"message": "Rule deleted", "rule_id": rule_id}
