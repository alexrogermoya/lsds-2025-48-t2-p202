from kafka import KafkaConsumer
import json
import os
import threading
import time
import requests

rules_view = {}

KAFKA_BROKER = os.getenv("BROKER", "kafka:9092")
METRICS_TOPIC = "metrics"

def send_alarm_to_discord(webhook_url, rule_id, metric, value, threshold):
    if not webhook_url:
        return

    message = {
        "embeds": [
            {
                "title": "🚨 Alarm Triggered! 🚨",
                "description": f"Metric **{metric}** exceeded the threshold.",
                "color": 16711680,  # Red
                "fields": [
                    {"name": "Rule ID", "value": rule_id},
                    {"name": "Metric", "value": metric},
                    {"name": "Value", "value": str(value)},
                    {"name": "Threshold", "value": str(threshold)}
                ]
            }
        ]
    }

    try:
        response = requests.post(webhook_url, json=message)
        if response.status_code == 204:
            print(f"✅ Alarm sent for rule {rule_id}.")
        else:
            print(f"❌ Error sending alarm: {response.status_code}")
    except Exception as e:
        print(f"❌ Error connecting to Discord: {e}")

def consume_rules():
    global rules_view
    consumer = KafkaConsumer(
        "rules",
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )

    print("✅ Listening for rules...")
    for message in consumer:
        rule_id = message.key
        new_rule = message.value

        if new_rule is None:
            rules_view.pop(rule_id, None)
            print(f"❌ Rule {rule_id} deleted.")
        else:
            rules_view[rule_id] = new_rule
            print(f"🔄 Rule {rule_id} updated: {new_rule}")

def consume_metrics():
    consumer = KafkaConsumer(
        METRICS_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )

    print("✅ Listening for metrics...")
    for message in consumer:
        metric_data = message.value
        metric = metric_data.get("metric")
        value = metric_data.get("value")

        if metric is None or value is None:
            continue

        for rule_id, rule in rules_view.items():
            if metric == rule["metric"]:
                condition = rule["condition"]
                threshold = rule["threshold"]
                if (condition == ">" and value > threshold) or \
                   (condition == "<" and value < threshold) or \
                   (condition == "==" and value == threshold):
                    print(f"🚨 Alarm triggered for rule {rule_id}: {metric} ({value}) {condition} {threshold}")
                    send_alarm_to_discord(rule.get("discord_webhook_url"), rule_id, metric, value, threshold)

threading.Thread(target=consume_rules, daemon=True).start()
threading.Thread(target=consume_metrics, daemon=True).start()

while True:
    time.sleep(1)
    