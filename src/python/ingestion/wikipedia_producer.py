import requests
import json
import os
from kafka import KafkaProducer
from datetime import datetime

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "pkc-7prvp.centralindia.azure.confluent.cloud:9092")
KAFKA_KEY       = os.environ.get("KAFKA_KEY", "")
KAFKA_SECRET    = os.environ.get("KAFKA_SECRET", "")
TOPIC           = "tweetpulse.raw.tweets"

HEADERS = {
    "User-Agent": "TweetPulse/1.0 (Learning project; nitantvaidya@outlook.com)"
}

def build_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=KAFKA_KEY,
        sasl_plain_password=KAFKA_SECRET,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8")
    )

def stream_to_kafka(count=100):
    producer  = build_producer()
    url       = "https://stream.wikimedia.org/v2/stream/recentchange"
    sent      = 0

    print(f"Streaming Wikipedia edits to Kafka topic: {TOPIC}")
    print(f"Target: {count} records")
    print("=" * 60)

    while sent < count:
        try:
            print(f"Connecting to Wikipedia stream... ({sent}/{count} sent so far)")
            with requests.get(url, stream=True, headers=HEADERS, timeout=60) as r:
                for line in r.iter_lines():
                    if sent >= count:
                        break
                    if line:
                        line = line.decode("utf-8")
                        if line.startswith("data:"):
                            try:
                                d = json.loads(line[5:])
                                if d.get("type") == "edit" and d.get("wiki") == "enwiki":
                                    record = {
                                        "id":            str(d.get("revision", {}).get("new", sent)),
                                        "text":          d.get("comment", "")[:280],
                                        "title":         d.get("title", ""),
                                        "author":        d.get("user", ""),
                                        "wiki":          d.get("wiki", ""),
                                        "created_at":    datetime.utcnow().isoformat(),
                                        "bytes_changed": d.get("length", {}).get("new", 0),
                                        "is_bot":        d.get("bot", False),
                                        "url":           d.get("meta", {}).get("uri", ""),
                                        "hashtags":      ["wikipedia", "enwiki"],
                                        "like_count":    0,
                                        "retweet_count": 0,
                                        "is_vandalism":  None
                                    }
                                    producer.send(TOPIC, key=record["id"], value=record)
                                    sent += 1
                                    print(f"[{sent}/{count}] Sent: {record['title'][:50]} | {record['author']}")
                            except Exception:
                                pass

        except Exception as e:
            if sent < count:
                print(f"Connection dropped at {sent}/{count} — reconnecting in 3 seconds...")
                import time
                time.sleep(3)
            else:
                break

    producer.flush()
    producer.close()
    print("=" * 60)
    print(f"Done! Sent {sent} records to Kafka")

if __name__ == "__main__":
    import sys
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    stream_to_kafka(count=count)