import requests
import uuid
import random
import time
from datetime import datetime

TARGET_URL = "http://aggregator:8080/publish"

TOTAL_EVENTS = 20000
DUPLICATE_RATIO = 0.35
BATCH_SIZE = 100
SLEEP_BETWEEN_BATCH = 0.05
MAX_RETRIES = 30


def wait_for_aggregator():
    print("[PUBLISHER] Waiting for aggregator to be ready...")
    for i in range(MAX_RETRIES):
        try:
            r = requests.get("http://aggregator:8080/docs", timeout=2)
            if r.status_code == 200:
                print("[PUBLISHER] Aggregator is ready")
                return
        except Exception:
            pass

        time.sleep(1)

    raise RuntimeError("Aggregator not ready after waiting")


def generate_events():
    unique_count = int(TOTAL_EVENTS * (1 - DUPLICATE_RATIO))
    duplicate_count = TOTAL_EVENTS - unique_count

    unique_ids = [str(uuid.uuid4()) for _ in range(unique_count)]
    duplicate_ids = random.choices(unique_ids, k=duplicate_count)

    all_ids = unique_ids + duplicate_ids
    random.shuffle(all_ids)

    events = []
    for eid in all_ids:
        events.append({
            "topic": "log",
            "event_id": eid,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source": "publisher-generator",
            "payload": {
                "message": "load test event",
                "value": random.randint(1, 100)
            }
        })

    return events


def main():
    wait_for_aggregator()

    events = generate_events()
    start_time = time.time()

    print(f"[PUBLISHER] Sending {len(events)} events "
          f"({int(DUPLICATE_RATIO*100)}% duplicates)")

    for i in range(0, len(events), BATCH_SIZE):
        batch = events[i:i + BATCH_SIZE]

        payload = {"events": batch}

        response = requests.post(TARGET_URL, json=payload)

        if response.status_code != 200:
            print("[ERROR]", response.text)
            break

        if i % 1000 == 0:
            print(f"[PUBLISHER] Sent {i} events")

        time.sleep(SLEEP_BETWEEN_BATCH)

    elapsed = time.time() - start_time
    print(f"[PUBLISHER] Done in {elapsed:.2f} seconds")
    print(f"[PUBLISHER] Throughput: {len(events)/elapsed:.2f} events/sec")


if __name__ == "__main__":
    main()
