import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

EVENT_TYPES = ["clic", "achat", "panier_abandonne"]

def generate_event():
    event_type = random.choice(EVENT_TYPES)
    event = {
        "event_type": event_type,
        "user_id": random.randint(1, 100),
        "timestamp": int(time.time()),
        "amount": round(random.uniform(10, 500), 2) if event_type == "achat" else None
    }
    return event

if __name__ == "__main__":
    while True:
        event = generate_event()
        producer.send("events", event)
        print(f"Sent: {event}")
        time.sleep(1)