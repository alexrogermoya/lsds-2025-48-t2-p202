from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from pydantic import BaseModel

import json
import uuid

# Initialize FastAPI app
app = FastAPI()

# Configure Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:19092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

rules_db = {}

# Define rule model
class RuleRequest(BaseModel):
    metric: str
    condition: str
    threshold: float

@app.post("/rules")
def create_rule(rule: RuleRequest):
    rule_id = str(uuid.uuid4())
    rule_data = {
        "id": rule_id,
        "metric": rule.metric,
        "condition": rule.condition,
        "threshold": rule.threshold,
    }
    producer.send("rules", key=rule_id, value=rule_data)
    return {"message": "Rule created", "rule": rule_data}