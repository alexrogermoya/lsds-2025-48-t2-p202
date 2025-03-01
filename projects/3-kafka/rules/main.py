from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from pydantic import BaseModel

import json
import uuid

# Initialize FastAPI app
app = FastAPI()

rules_db = {}

# Configure Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:19092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8") if v is not None else None,
    key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
)

# Define Rule Model
class RuleRequest(BaseModel):
    metric: str
    condition: str
    threshold: float

@app.post("/rules")
def create_rule(rule: RuleRequest):
    global rules_db 

    rule_id = str(uuid.uuid4())
    rule_data = {
        "id": rule_id,
        "metric": rule.metric,
        "condition": rule.condition,
        "threshold": rule.threshold,
    }

    rules_db[rule_id] = rule_data

    print("Current rules_db:", rules_db)

    producer.send("rules", key=rule_id.encode("utf-8"), value=rule_data)
    producer.flush()

    return {"message": "Rule created", "rule": rule_data}

@app.delete("/rules/{rule_id}")
def delete_rule(rule_id: str):
    global rules_db

    if rule_id not in rules_db:
        raise HTTPException(status_code=404, detail="Rule not found")

    # Remove rule from memory
    del rules_db[rule_id]

    # Send a deletion event to Kafka (null message)
    producer.send("rules", key=rule_id.encode("utf-8"), value=None)
    producer.flush()

    print(f"Deleted rule: {rule_id}")

    return {"message": "Rule deleted", "rule_id": rule_id}