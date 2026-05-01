import requests
import json
import os
import time
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

def fetch_reverted_edits(limit=500, rccontinue=None):
    params = {
        "action":  "query",
        "list":    "recentchanges",
        "rctype":  "edit",
        "rcprop":  "title|ids|sizes|flags|user|comment|timestamp",
        "rctag":   "mw-reverted",
        "rclimit": limit,
        "format":  "json"
    }
    if rccontinue:
        params["rccontinue"] = rccontinue
    response = requests.get(
        "https://en.wikipedia.org/w/api.php",
        params=params,
        headers=HEADERS
    )
    return response.json()

def fetch_legitimate_edits(limit=500, rccontinue=None):
    params = {
        "action":  "query",
        "list":    "recentchanges",
        "rctype":  "edit",
        "rcprop":  "title|ids|sizes|flags|user|comment|timestamp",
        "rcshow":  "!bot|!minor",
        "rclimit": limit,
        "format":  "json"
    }
    if rccontinue:
        params["rccontinue"] = rccontinue
    response = requests.get(
        "https://en.wikipedia.org/w/api.php",
        params=params,
        headers=HEADERS
    )
    return response.json()

def edit_to_record(edit, is_vandalism):
    old_size     = edit.get("oldlen", 0) or 0
    new_size     = edit.get("newlen", 0) or 0
    bytes_changed = new_size - old_size
    return {
        "id":            str(edit.get("revid", "")),
        "text":          edit.get("comment", "")[:280],
        "title":         edit.get("title", ""),
        "author":        edit.get("user", ""),
        "wiki":          "enwiki",
        "created_at":    edit.get("timestamp", datetime.utcnow().isoformat()),
        "bytes_changed": bytes_changed,
        "is_bot":        "bot" in edit.get("flags", []),
        "url":           f"https://en.wikipedia.org/wiki/{edit.get('title','').replace(' ','_')}",
        "hashtags":      ["wikipedia", "enwiki"],
        "like_count":    0,
        "retweet_count": 0,
        "is_vandalism":  is_vandalism
    }

def produce_labeled_edits(target_count=5000):
    producer = build_producer()
    sent             = 0
    vandalism_count  = 0
    legitimate_count = 0

    target_vandalism  = int(target_count * 0.3)
    target_legitimate = target_count - target_vandalism

    print(f"Target: {target_vandalism} vandalism + {target_legitimate} legitimate")
    print("=" * 60)

    # ── Fetch reverted (vandalism) edits ───────────────────────
    print("Fetching reverted (vandalism) edits...")
    rccontinue = None
    while vandalism_count < target_vandalism:
        try:
            data  = fetch_reverted_edits(
                limit=min(500, target_vandalism - vandalism_count),
                rccontinue=rccontinue
            )
            edits = data.get("query", {}).get("recentchanges", [])
            if not edits:
                print("No more reverted edits available")
                break
            for edit in edits:
                if vandalism_count >= target_vandalism:
                    break
                record = edit_to_record(edit, is_vandalism=True)
                producer.send(TOPIC, key=record["id"], value=record)
                vandalism_count += 1
                sent += 1
                if vandalism_count % 100 == 0:
                    print(f"  Vandalism: {vandalism_count}/{target_vandalism}")
            rccontinue = data.get("continue", {}).get("rccontinue")
            if not rccontinue:
                print("No more pages for vandalism edits")
                break
            time.sleep(1)
        except Exception as e:
            print(f"Error fetching vandalism edits: {e}")
            time.sleep(5)

    # ── Fetch legitimate edits ──────────────────────────────────
    print(f"\nFetching legitimate edits...")
    rccontinue = None
    while legitimate_count < target_legitimate:
        try:
            data  = fetch_legitimate_edits(
                limit=min(500, target_legitimate - legitimate_count),
                rccontinue=rccontinue
            )
            edits = data.get("query", {}).get("recentchanges", [])
            if not edits:
                print("No more legitimate edits available")
                break
            for edit in edits:
                if legitimate_count >= target_legitimate:
                    break
                record = edit_to_record(edit, is_vandalism=False)
                producer.send(TOPIC, key=record["id"], value=record)
                legitimate_count += 1
                sent += 1
                if legitimate_count % 500 == 0:
                    print(f"  Legitimate: {legitimate_count}/{target_legitimate}")
            rccontinue = data.get("continue", {}).get("rccontinue")
            if not rccontinue:
                print("No more pages for legitimate edits")
                break
            time.sleep(1)
        except Exception as e:
            print(f"Error fetching legitimate edits: {e}")
            time.sleep(5)

    producer.flush()
    producer.close()

    print("=" * 60)
    print(f"Done!")
    print(f"Vandalism records sent:  {vandalism_count}")
    print(f"Legitimate records sent: {legitimate_count}")
    print(f"Total sent:              {sent}")

if __name__ == "__main__":
    import sys
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    produce_labeled_edits(target_count=count)